using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace DataLoader
{
    class Parameter
    {
        private string _provider;
        private string _connectionString;
        private string _inputFile;
        private string _outputFile;

        public Parameter()
        {
            _provider = ConfigurationManager.AppSettings["Provider"];
            _connectionString = ConfigurationManager.AppSettings["ConnectionString"];
            _inputFile = ConfigurationManager.AppSettings["InputFile"];
            _outputFile = ConfigurationManager.AppSettings["OutputFile"];
        }

        public bool Valid 
        {
            get
            {
                bool valid = true;
                
                valid = valid && !string.IsNullOrEmpty(this.Provider);
                valid = valid && !string.IsNullOrEmpty(this.ConnectionString);
                valid = valid && !string.IsNullOrEmpty(this.InputFile);

                return valid;
            }
        }
        public string Provider 
        {
            get { return _provider; }
            set { _provider = value; }
        }

        public string ConnectionString 
        {
            get { return _connectionString; }
            set { _connectionString = value; }
        }

        public string InputFile
        {
            get { return _inputFile; }
            set { _inputFile = value; }
        }

        public string OutputFile 
        {
            get { return _outputFile; }
            set { _outputFile = value; }
        }
    }
}
