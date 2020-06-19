using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using MongoDB.Bson;

namespace SqlToMongoCore
{
    public class TableMapping
    {
        public const string nodeName = "Table";
        protected const string attributeSQLTable = "SQLTable";
        protected const string attributePrimaryKeyColumn = "PrimaryKeyColumn";
        protected const string attributeMongoCollection = "MongoCollection";
        protected const string attributePageSize = "PageSize";
        protected const string attributeLastSyncIdentity = "LastSyncIdentity";
        protected const string attributeIsSelected = "IsSelected";
        public const string ColumnRowID = "RowID";

        public delegate BsonDocument ConvertBsonDocumentDelegate(IDataReader reader, string primaryKeyColumn);

        #region Properties

        /// <summary>
        /// Gets or sets the SQL table.
        /// </summary>
        /// <value>
        /// The SQL table.
        /// </value>
        public string SQLTable
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the mongo collection.
        /// </summary>
        /// <value>
        /// The mongo collection.
        /// </value>
        public string MongoCollection
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the size of the page.
        /// For large size database, this setting can help to improve performence when doing translation.
        /// Default value is 500, which is applied only when <see cref="OrderByColumn"/> is set.
        /// </summary>
        /// <value>
        /// The size of the page.
        /// </value>
        public int PageSize
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the last sync identity.
        /// </summary>
        /// <value>
        /// The last sync identity.
        /// </value>
        public string LastSyncIdentity
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the primary key column.
        /// </summary>
        /// <value>
        /// The primary key column.
        /// </value>
        public string PrimaryKeyColumn
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is selected.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is selected; otherwise, <c>false</c>.
        /// </value>
        public bool IsSelected
        {
            get;
            set;
        }

        #endregion

        /// <summary>
        /// To the XML.
        /// </summary>
        /// <returns></returns>
        public XElement ToXml()
        {
            XElement element = this.CreateXmlNode(nodeName);

            element.SetAttributeValue(attributePrimaryKeyColumn, this.PrimaryKeyColumn.DBToString());
            element.SetAttributeValue(attributeIsSelected, this.IsSelected.ToString());
            element.SetAttributeValue(attributeLastSyncIdentity, this.LastSyncIdentity.DBToString());
            element.SetAttributeValue(attributePageSize, this.PageSize.ToString());
            element.SetAttributeValue(attributeMongoCollection, this.MongoCollection.DBToString());
            element.SetAttributeValue(attributeSQLTable, this.SQLTable.DBToString());

            return element;
        }

        /// <summary>
        /// Gets the name of the mongo column.
        /// </summary>
        /// <param name="propertyName">Name of the property.</param>
        /// <returns></returns>
        public string GetMongoColumnName(string propertyName)
        {
            return (!string.IsNullOrWhiteSpace(this.PrimaryKeyColumn) && propertyName == this.PrimaryKeyColumn) ? "_id" : propertyName;
        }

        /// <summary>
        /// Gets the delegate for converting bson document.
        /// </summary>
        /// <param name="schema">The schema.</param>
        /// <returns></returns>
        public ConvertBsonDocumentDelegate GetConvertBsonDocumentDelegate(Dictionary<string, string> schema)
        {
            ConvertBsonDocumentDelegate result = null;

            if (schema != null)
            {
                result = new ConvertBsonDocumentDelegate(delegate(IDataReader reader, string primaryKeyColumn)
                {
                    BsonDocument bsonDocument = new BsonDocument();
                    object dbObj;
                    BsonValue bsonValue;

                    foreach (string key in schema.Keys)
                    {
                        dbObj = reader[key];
                        bsonDocument.Add(GetMongoColumnName(key), BsonTypeMapper.TryMapToBsonValue(dbObj, out bsonValue) ? bsonValue : dbObj.DBToString());
                    }

                    dbObj = reader[ColumnRowID];
                    bsonDocument.Add(ColumnRowID, BsonTypeMapper.TryMapToBsonValue(dbObj, out bsonValue) ? bsonValue : dbObj.DBToString());

                    return bsonDocument;
                });
            }

            return result;
        }

        /// <summary>
        /// Checks the page size and order by column.
        /// </summary>
        public void CheckPageSizeAndOrderByColumn()
        {
            if (PageSize <= 0)
            {
                PageSize = 500;
            }
        }

        /// <summary>
        /// Loads from XML.
        /// </summary>
        /// <param name="xElement">The x element.</param>
        /// <returns></returns>
        public static TableMapping LoadFromXml(XElement xElement)
        {
            TableMapping mapping = null;

            if (xElement != null && xElement.Name.LocalName == nodeName)
            {
                mapping = new TableMapping();

                bool isSelected = true;
                Boolean.TryParse(xElement.GetAttributeValue(attributeIsSelected), out isSelected);
                mapping.IsSelected = isSelected;

                int pageSize;
                Int32.TryParse(xElement.GetAttributeValue(attributePageSize), out pageSize);
                mapping.PageSize = pageSize;

                mapping.LastSyncIdentity = xElement.GetAttributeValue(attributeLastSyncIdentity);
                mapping.MongoCollection = xElement.GetAttributeValue(attributeMongoCollection);
                mapping.SQLTable = xElement.GetAttributeValue(attributeSQLTable);
                mapping.PrimaryKeyColumn = xElement.GetAttributeValue(attributePrimaryKeyColumn);
            }

            return mapping;
        }
    }
}
