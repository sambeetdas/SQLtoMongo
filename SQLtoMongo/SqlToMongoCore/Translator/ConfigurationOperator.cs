using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace SqlToMongoCore
{
    public class ConfigurationOperator
    {
        #region Properties

        /// <summary>
        /// Gets the mappings.
        /// </summary>
        /// <value>
        /// The mappings.
        /// </value>
        public TranslatorSettings TranslatorSettings
        {
            get;
            protected set;
        }

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigurationOperator"/> class.
        /// </summary>
        /// <param name="encodedXmlString">The encoded XML string.</param>
        public ConfigurationOperator(string encodedXmlString)
            : this()
        {
            string content = encodedXmlString.DecodeBase64();
            if (!string.IsNullOrWhiteSpace(content))
            {
                LoadDataFromXml(XDocument.Parse(content));
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigurationOperator"/> class.
        /// </summary>
        /// <param name="xElement">The x element.</param>
        public ConfigurationOperator(XDocument xElement)
            : this()
        {
            LoadDataFromXml(xElement);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigurationOperator"/> class.
        /// </summary>
        public ConfigurationOperator()
        {
        }

        #endregion


        /// <summary>
        /// Loads the data from XML.
        /// </summary>
        /// <param name="rootNode">The root node.</param>
        protected void LoadDataFromXml(XDocument xDocument)
        {
            if (xDocument != null && xDocument.Root.Name.LocalName == "SQLToMongo")
            {
                XElement xElement = xDocument.Root.Element("Mapping");
                if (xElement != null)
                {
                    try
                    {
                        this.TranslatorSettings = TranslatorSettings.LoadFromXml(xElement);
                    }
                    catch { }
                }
            }

            if (this.TranslatorSettings == null)
            {
                this.TranslatorSettings = new TranslatorSettings();
            }
        }

        /// <summary>
        /// To the XML.
        /// </summary>
        /// <returns></returns>
        public XDocument ToXml()
        {
            XDocument xDocument = XDocument.Parse(@"<?xml version=""1.0"" encoding=""utf-8"" ?>
<SQLToMongo></SQLToMongo>");
            xDocument.Root.Add(this.TranslatorSettings.ToXml());
            return xDocument;
        }

        public void Save(string path)
        {
            XDocument document = this.ToXml();
            document.Save(path);
        }
    }
}
