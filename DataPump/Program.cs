using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataLoader
{
    class Program
    {
        private static Parameter ParseParameter(string[] args)
        {
// -p "FirebirdSql.Data.FirebirdClient" -c "initial catalog=efa-test.m.mdv:C:\MentzDV\Programme\EFAPersonalization\Data\PERSONALIZATION.FDB;server type=Default;data source=localhost;user id=sysdba;password=masterkey;charset=UTF8;" -i "I:\DataPump\DataPump\TestData\DataOut.xml"

            var parameter = new Parameter();
            try
            {
                for (int i = 0; i < args.Length; i++)
                {
                    if (args[i] == "-p" && i < args.Length - 1)
                    {
                        parameter.Provider = args[i + 1];
                        i++;
                    }

                    else if (args[i] == "-c" && i < args.Length - 1)
                    {
                        parameter.ConnectionString = args[i + 1];
                        i++;
                    }

                    else if (args[i] == "-i" && i < args.Length - 1)
                    {
                        parameter.InputFile = args[i + 1];
                        i++;
                    }

                    else if (args[i] == "-o" && i < args.Length - 1)
                    {
                        parameter.OutputFile = args[i + 1];
                        i++;
                    }
                }
            }
            catch
            {
                parameter.Provider = string.Empty;
                parameter.OutputFile = string.Empty;
                parameter.InputFile = string.Empty;
                parameter.ConnectionString = string.Empty;
            }

            return parameter;
        }

        static void Main(string[] args)
        {
            var parameter = ParseParameter(args);

            if (!parameter.Valid)
            {
                Console.WriteLine("usage: [-d] [-o outputFile] -i inputFile -p providerName -c connectionString");
            }
            else
            {
                Console.Title = "Data Pump";
                Console.CursorVisible = false;
                Console.WriteLine("Data Pump v0.1");

                try
                {
                    if (args.Contains("-d"))
                    {
                        var dumper = new DataPumpOut(parameter.Provider, parameter.ConnectionString, parameter.InputFile);
                        dumper.Process();
                        dumper.Save(parameter.OutputFile ?? parameter.InputFile);
                    }
                    else 
                    {
                        var loader = new DataPumpIn(parameter.Provider, parameter.ConnectionString, parameter.InputFile);
                        loader.Process();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error:" + e.Message);
                }

                Console.CursorVisible = true;
            }
        }
    }
}
