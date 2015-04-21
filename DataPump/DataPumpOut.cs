using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace DataLoader
{
    class DataPumpOut : DataPump
    {
        private int _reads = 0;

        public DataPumpOut(string provider, string connection, string file) : 
            base(provider, connection, file)
        {
            foreach (var table in _tablesNode.Elements(XName.Get("table")))
            {
                var rows = GetRowCount(table);
                CountRows(table, rows);
                _totalRows += rows;
            }
        }

        private void CountRows(XElement parentNode, int parentRows)
        {
            foreach (var childNode in parentNode.Elements(XName.Get("table")))
            {
                int count = GetRowCount(childNode) * parentRows;
                CountRows(childNode, count);
                _totalRows += count;
            }
        }

        public void Save(string outputFile)
        {
            _doc.Save(outputFile);
        }

        public override void ProcessTable(XElement table, XElement dataNode, string fkName, int id)
        {
            var tableName = table.Attribute(XName.Get("name")).Value;
            var query = table.Attribute(XName.Get("query"));
            var tableKey = "ID";

            if (table.Attribute(XName.Get("id")) != null)
            {
                tableKey = table.Attribute(XName.Get("id")).Value;
            }

            if (table.Attribute(XName.Get("fk")) != null)
            {
                fkName = table.Attribute(XName.Get("fk")).Value;
            }

            ReportProgress(tableName, _reads);

            var command = _connection.CreateCommand();
            if (query == null)
            {
                command.CommandText = "SELECT * FROM " + tableName;
                if (!string.IsNullOrEmpty(fkName))
                {
                    command.CommandText += " WHERE " + fkName + "='" + id + "'";
                }
            }
            else
            {
                command.CommandText = query.Value.Replace("@fk", id.ToString());
            }

            id = 0;
            int rows = 0, totalRows = Convert.ToInt32(table.Attribute(XName.Get("rows")).Value);

            var fkIds = new List<Tuple<int, XElement>>();

            using (var reader = command.ExecuteReader())
            {
                var reads = _reads;

                while (reader.Read())
                {
                    var rowNode = new XElement(XName.Get("row"));
                    dataNode.Add(rowNode);

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        // column
                        var columnName = reader.GetName(i);
                        var columnValue = reader.GetValue(i);
                        var columnType = reader.GetFieldType(i);

                        if (columnName.ToUpper() == tableKey)
                        {
                            id = Convert.ToInt32(columnValue);
                        }
                        else if (columnName.ToUpper() != fkName && !(columnValue is DBNull))
                        {
                            var columnNode = new XElement(XName.Get("column"));
                            columnNode.Add(new XAttribute(XName.Get("name"), columnName));
                            columnNode.Add(new XAttribute(XName.Get("type"), ToDbType(columnType)));

                            if (columnType == typeof(DateTime))
                            {
                                columnNode.Add(new XText(((DateTime)columnValue).ToString("o")));
                            }
                            else
                            {
                                columnNode.Add(new XText(columnValue.ToString()));
                            }

                            rowNode.Add(columnNode);
                        }
                    }

                    fkIds.Add(Tuple.Create(id, rowNode));

                    rows++;
                    _reads++;
                    ReportProgress(tableName, _reads);

                    if (rows == totalRows)
                    {
                        break;
                    }
                }

                _reads = reads + totalRows;
            }

            fkName = tableName.ToUpper() + "_ID";

            foreach (var fkId in fkIds)
            {
                foreach (var childTable in table.Elements(XName.Get("table")))
                {
                    var childNode = new XElement(XName.Get("table"));
                    fkId.Item2.Add(childNode);
                    ProcessTable(childTable, childNode, fkName, fkId.Item1);
                }
            }
        }

        private static string ToDbType(Type typeInfo)
        {
            switch (typeInfo.ToString())
            {
                case "System.Int16":
                    return "short";
                case "System.Int32":
                    return "int";
                case "System.Int64":
                    return "long";
                case "System.Byte":
                    return "byte";
                case "System.String":
                    return "string";
                case "System.DateTime":
                    return "date";
            }

            return typeInfo.ToString();
        }
    }
}
