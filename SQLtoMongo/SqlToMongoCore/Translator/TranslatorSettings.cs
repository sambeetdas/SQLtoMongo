using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace SqlToMongoCore
{
    /// <summary>
    /// Class for translator settings.
    /// </summary>
    public class TranslatorSettings
    {
        protected const string nodeName = "Mapping";
        protected const string attributeSQLConnection = "SQLConnection";
        protected const string attributeMongoConnection = "MongoConnection";
        protected const string attributePageSize = "PageSize";


        #region Properties

        /// <summary>
        /// Gets or sets the SQL connection string.
        /// </summary>
        /// <value>
        /// The SQL connection string.
        /// </value>
        public string SqlConnectionString
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the mongo connection string.
        /// </summary>
        /// <value>
        /// The mongo connection string.
        /// </value>
        public string MongoConnectionString
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the mappings.
        /// </summary>
        /// <value>
        /// The mappings.
        /// </value>
        public List<TableMapping> Mappings
        {
            get;
            protected set;
        }

        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="TranslatorSettings"/> class.
        /// </summary>
        public TranslatorSettings()
        {
            this.Mappings = new List<TableMapping>();
        }


        /// <summary>
        /// Formats the name of the column.
        /// </summary>
        /// <param name="columnName">Name of the column.</param>
        /// <returns></returns>
        protected string FormatColumnName(string columnName)
        {
            return !string.IsNullOrWhiteSpace(columnName) ?
                string.Format("[{0}]", columnName.Trim(new char[] { ' ', '[', ']' })) : string.Empty;
        }

        /// <summary>
        /// To the XML.
        /// </summary>
        /// <returns></returns>
        public XElement ToXml()
        {
            XElement element = this.CreateXmlNode(nodeName);

            element.SetAttributeValue(attributeMongoConnection, this.MongoConnectionString.DBToString());
            element.SetAttributeValue(attributeSQLConnection, this.SqlConnectionString.DBToString());

            foreach (var one in this.Mappings)
            {
                element.Add(one.ToXml());
            }

            return element;
        }

        /// <summary>
        /// Loads from XML.
        /// </summary>
        /// <param name="xElement">The x element.</param>
        /// <returns></returns>
        public static TranslatorSettings LoadFromXml(XElement xElement)
        {
            TranslatorSettings settings = null;

            if (xElement != null && xElement.Name.LocalName == nodeName)
            {
                settings = new TranslatorSettings();
                settings.SqlConnectionString = xElement.GetAttributeValue(attributeSQLConnection);
                settings.MongoConnectionString = xElement.GetAttributeValue(attributeMongoConnection);

                foreach (var one in xElement.Elements(TableMapping.nodeName))
                {
                    TableMapping mapping = TableMapping.LoadFromXml(one);
                    if (mapping != null)
                    {
                        settings.Mappings.Add(mapping);
                    }
                }
            }

            return settings;
        }
    }
}
