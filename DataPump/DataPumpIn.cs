using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace DataLoader
{
    class DataPumpIn : DataPump
    {
        private int _inserts = 0;
        private Dictionary<string, int> _ids = new Dictionary<string, int>();

        public DataPumpIn(string provider, string connection, string file) :
            base(provider, connection, file)
        {

            var tableNodes = _tablesNode.Elements(XName.Get("table"));
            var dataNodes = _dumpNode.Elements(XName.Get("table"));

            System.Diagnostics.Debug.Assert(tableNodes.Count() == dataNodes.Count());

            for (int i = 0; i < Math.Min(tableNodes.Count(), dataNodes.Count()); i++)
            {
                var tableNode = tableNodes.ElementAt(i);
                var dataNode = dataNodes.ElementAt(i);

                int rows = 0;

                if (tableNode.Attribute(XName.Get("rows")) != null && tableNode.Attribute(XName.Get("rows")).Value != "*") 
                {
                    rows = GetRowCount(tableNode);
                } 
                else 
                {
                    rows = dataNode.Elements(XName.Get("row")).Count();
                }
                CountRows(tableNode, dataNode, rows);
                _totalRows += rows;
            }
        }

        private void CountRows(XElement tableNode, XElement dataNode, int parentRows)
        {
            var tableNodes = tableNode.Elements(XName.Get("table"));
            var dataRows = dataNode.Elements(XName.Get("row"));

            for (int i = 0; i < tableNodes.Count(); i++)
            {
                tableNode = tableNodes.ElementAt(i);
                int rows = 0;

                if (tableNode.Attribute(XName.Get("rows")) != null && tableNode.Attribute(XName.Get("rows")).Value != "*")
                {
                    rows = GetRowCount(tableNode) * parentRows;
                    CountRows(tableNode, dataNode, rows);
                    _totalRows += rows;
                }
                else
                {
                    rows = dataRows.Count();
                    foreach (var dataRow in dataRows)
                    {
                        CountRows(tableNode, dataRow.Elements(XName.Get("table")).ElementAt(i), rows);
                    }
                    _totalRows += rows;
                }
            }
        }

        public override void ProcessTable(XElement table, XElement dataNode, string fkName, int parentId)
        {
            var tableName = table.Attribute(XName.Get("name")).Value;
            var tableKey = "ID";

            if (table.Attribute(XName.Get("id")) != null)
            {
                tableKey = table.Attribute(XName.Get("id")).Value;
            }

            if (table.Attribute(XName.Get("fk")) != null)
            {
                fkName = table.Attribute(XName.Get("fk")).Value;
            }

            var rows = dataNode.Elements(XName.Get("row"));
            var rowsCount = rows.Count();
            var tableRows = rowsCount;

            if (table.Attribute(XName.Get("rows")) != null && table.Attribute(XName.Get("rows")).Value != "*")
            {
                if (tableRows != 0)
                {
                    tableRows = Convert.ToInt32(table.Attribute(XName.Get("rows")).Value);
                }
                else
                {
                    _inserts += Convert.ToInt32(table.Attribute(XName.Get("rows")).Value);
                }
            }
            ReportProgress(tableName, _inserts);

            for (int i = 0; rowsCount > 0 && i < tableRows; i++)
            {
                var random = new Random();
                var row = rows.ElementAt(i % rowsCount);

                int newParentId = 0;

                using (var command = CreateCommand(tableName, tableKey, fkName, row))
                {
                    if (!string.IsNullOrEmpty(fkName) && parentId != 0)
                    {
                        var parameter = command.CreateParameter();
                        parameter.DbType = DbType.Int32;
                        parameter.ParameterName = "@" + fkName;
                        parameter.Direction = ParameterDirection.Input;
                        parameter.Value = parentId;

                        command.Parameters.Add(parameter);
                    }

                    SetParameters(row, tableKey, fkName, command);
                    command.ExecuteNonQuery();
                }

                newParentId = GetPk(table, tableName, tableKey);

                _inserts++;
                ReportProgress(tableName, _inserts);

                var tables = table.Elements(XName.Get("table"));
                var dataNodes = row.Elements(XName.Get("data"));

                System.Diagnostics.Debug.Assert(tables.Count() == dataNodes.Count());

                for (int k = 0; k < Math.Min(tables.Count(), dataNodes.Count()); k++)
                {
                    ProcessTable(tables.ElementAt(k), dataNodes.ElementAt(k), tableName.ToUpper() + "_ID", newParentId);
                }
            }
        }

        private int GetPk(XElement table, string tableName, string tableKey)
        {
            int pk = 0;

            string commandText;
            if (table.Attribute(XName.Get("identity")) == null)
            {
                commandText = "SELECT max(" + tableKey + ") FROM " + tableName;
            }
            else
            {
                commandText = table.Attribute(XName.Get("identity")).Value;
            }

            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT CURRENT_VALUE FROM TABLE_KEYS WHERE TABLE_NAME='" + tableName + "'";
                pk = Convert.ToInt32(command.ExecuteScalar());
            }

            return pk;
        }

        private void SetParameters(XElement row, string tableKey, string fkName, DbCommand command)
        {
            foreach (var column in row.Elements(XName.Get("column")))
            {
                var name = column.Attribute(XName.Get("name")).Value;
                var type = column.Attributes(XName.Get("type"));

                if (name == tableKey || name == fkName)
                    continue;

                var parameter = command.CreateParameter();
                parameter.ParameterName = "@" + name;

                if (type.Count() > 0)
                {
                    parameter.DbType = GetDbType(type.First().Value);
                    parameter.Value = GetDbValue(parameter.DbType, column.Value);
                }
                else
                {
                    var table = column.Attributes(XName.Get("table"));
                    if (table.Count() > 0)
                    {
                        parameter.DbType = DbType.Int32;
                        parameter.Value = _ids[table.First().Value];
                    }
                    else
                    {
                        parameter.DbType = DbType.String;
                        parameter.Value = GetDbValue(parameter.DbType, column.Value);
                    }
                }
                parameter.Direction = ParameterDirection.Input;
                command.Parameters.Add(parameter);
            }
        }

        private DbType GetDbType(string type)
        {
            switch (type)
            {
                case "string":
                    return DbType.AnsiString;
                case "byte":
                    return DbType.Byte;
                case "binary":
                    return DbType.Binary;
                case "bool":
                    return DbType.Boolean;
                case "date":
                    return DbType.Date;
                case "decimal":
                    return DbType.Decimal;
                case "double":
                    return DbType.Double;
                case "short":
                    return DbType.Int16;
                case "int":
                    return DbType.Int32;
                case "long":
                    return DbType.Int64;
            }

            return DbType.String;
        }

        private object GetDbValue(DbType type, string value)
        {
            switch (type)
            {
                case DbType.AnsiString:
                    return value;
                case DbType.Byte:
                    return Convert.ToByte(value);
                case DbType.Binary:
                    return Convert.FromBase64String(value);
                case DbType.Boolean:
                    return (value == "1" || value == "true");
                case DbType.Date:
                    return Convert.ToDateTime(value);
                case DbType.Decimal:
                    return Convert.ToDecimal(value);
                case DbType.Double:
                    return Convert.ToDouble(value);
                case DbType.Int16:
                    return Convert.ToInt16(value);
                case DbType.Int32:
                    return Convert.ToInt32(value);
                case DbType.Int64:
                    return Convert.ToInt64(value);
            }

            return value;
        }

        private DbCommand CreateCommand(string tableName, string tableKey, string fkName, XElement row)
        {
            var command = _connection.CreateCommand();
            var builder = new StringBuilder();
            var columnNames = row.Elements(XName.Get("column")).Select(e => e.Attributes(XName.Get("name")).First().Value).Where(n => n != tableKey && n != fkName);

            builder.Append("INSERT INTO ");
            builder.Append(tableName);
            builder.Append(" (\"");
            if (!string.IsNullOrEmpty(fkName))
            {
                builder.Append(fkName);
                builder.Append("\",\"");
            }
            builder.Append(string.Join("\",\"", columnNames));
            builder.Append("\") VALUES(@");
            if (!string.IsNullOrEmpty(fkName))
            {
                builder.Append(fkName);
                builder.Append(",@");
            }
            builder.Append(string.Join(",@", columnNames));
            builder.Append(")");

            command.CommandText = builder.ToString();
            return command;
        }
    }
}
