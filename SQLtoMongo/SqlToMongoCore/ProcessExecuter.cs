using SqlToMongoCore.Properties;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace SqlToMongoCore
{
    public class ProcessExecuter
    {
        static StringBuilder logBuilder = new StringBuilder();
        public void Run(XDocument strXml)
        {
            XDocument xDocument = new XDocument(strXml);
            PreExecute(xDocument);         
        }

        public void PreExecute(XDocument xDocument)
        {
            ConfigurationOperator configurationOperator = new ConfigurationOperator(xDocument);
            Execute(configurationOperator);
        }

        private static void Execute(ConfigurationOperator configurationOperator)
        {
            Translator translator = new Translator(configurationOperator.TranslatorSettings);
            translator.Translate();
        }
    }
}
