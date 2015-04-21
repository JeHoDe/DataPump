using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace DataLoader
{
    abstract class DataPump
    {
        protected XDocument _doc = null;
        protected DbConnection _connection = null;

        protected XElement _dumpNode = null;
        protected XElement _tablesNode = null;
        protected int _totalRows = 0;

        private const int BarSize = 50;
        private const char BarChar = '=';

        public DataPump(string provider, string connection, string file)
        {
            _doc = XDocument.Load(file);

            var factory = DbProviderFactories.GetFactory(provider);

            _connection = factory.CreateConnection();
            _connection.ConnectionString = connection;

            var rootNode = _doc.Element(XName.Get("datapump"));

            _dumpNode = rootNode.Element(XName.Get("dump"));
            _tablesNode = rootNode.Element(XName.Get("tables"));

            if (_dumpNode == null)
            {
                _dumpNode = new XElement(XName.Get("dump"));
                rootNode.AddFirst(_dumpNode);
            }
        }

        protected static int GetRowCount(XElement node)
        {
            return Convert.ToInt32(node.Attribute(XName.Get("rows")).Value);
        }

        internal void Process()
        {
            _connection.Open();

            var tableNodes = _tablesNode.Elements(XName.Get("table"));
            var dataNodes = _dumpNode.Elements(XName.Get("data"));

            for (int i = 0; i < tableNodes.Count(); i++)
            {
                if (i < dataNodes.Count())
                {
                    ProcessTable(tableNodes.ElementAt(i), dataNodes.ElementAt(i), string.Empty, 0);
                }
                else
                {
                    var dataNode = new XElement(XName.Get("data"));
                    _dumpNode.Add(dataNode);
                    ProcessTable(tableNodes.ElementAt(i), dataNode, string.Empty, 0);
                }
            }

            _connection.Close();
        }

        protected void ReportProgress(string tableName, int complete)
        {
            int cursorLeft = Console.CursorLeft;
            double percent = Math.Min(complete, _totalRows) / (double)_totalRows;
            int chars = (int)Math.Floor(percent * (double)BarSize);
            string done = String.Empty, toDo = String.Empty;

            for (int i = 0; i < chars; i++)
            {
                done += BarChar;
            }

            for (int i = 0; i < BarSize - chars; i++)
            {
                toDo += BarChar;
            }

            Console.Write('[');
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write(done);
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.Write(toDo);
            Console.ResetColor();

            Console.Write("] {0,5}% {1:0000000}/{2:0000000}", (percent * 100).ToString("N2"), complete, _totalRows);
            Console.CursorLeft = cursorLeft;
            Console.Write("\nTable: {0, -40}", tableName);
            Console.SetCursorPosition(0, Console.CursorTop - 1);
        }

        public abstract void ProcessTable(XElement table, XElement dataNode, string fk, int id);
    }
}
