using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SqlToMongoCore
{
    public class Translator
    {
        public delegate List<T> ConvertListEntityDelegate<T>(IDataReader reader);
        public delegate Dictionary<TKey, TValue> ConvertDictionaryEntityDelegate<TKey, TValue>(IDataReader reader);

        #region Properties

        /// <summary>
        /// Gets a value indicating whether this instance has SQL connection string.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance has SQL connection string; otherwise, <c>false</c>.
        /// </value>
        public bool HasSQLConnectionString
        {
            get
            {
                return this.TranslatorSettings != null && !string.IsNullOrWhiteSpace(this.TranslatorSettings.SqlConnectionString);
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance has mongo DB connection string.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance has mongo DB connection string; otherwise, <c>false</c>.
        /// </value>
        public bool HasMongoDBConnectionString
        {
            get
            {
                return this.TranslatorSettings != null && !string.IsNullOrWhiteSpace(this.TranslatorSettings.MongoConnectionString);
            }
        }

        /// <summary>
        /// Gets or sets the translator settings.
        /// </summary>
        /// <value>
        /// The translator settings.
        /// </value>
        public TranslatorSettings TranslatorSettings
        {
            get;
            set;
        }

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="Translator"/> class.
        /// </summary>
        /// <param name="translatorSettings">The translator settings.</param>
        public Translator(TranslatorSettings translatorSettings)
        {
            this.TranslatorSettings = translatorSettings;
        }

        #endregion

        #region Translate

        /// <summary>
        /// Gets the schema from SQL.
        /// Key: Column name.
        /// Value: Column data type.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public Dictionary<string, string> GetSchemaFromSQL(string tableName)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();

            if (!string.IsNullOrWhiteSpace(tableName) && this.HasSQLConnectionString)
            {
                ConvertDictionaryEntityDelegate<string, string> convertDBSchema = new ConvertDictionaryEntityDelegate<string, string>(delegate(IDataReader reader)
                {
                    Dictionary<string, string> dbResult = new Dictionary<string, string>();
                    if (reader != null)
                    {
                        while (reader.Read())
                        {
                            dbResult.Add(reader["COLUMN_NAME"].DBToString(), reader["DATA_TYPE"].DBToString());
                        }
                    }
                    return dbResult;
                });

                string sql = "SELECT [Table_CATALOG], [COLUMN_NAME], [COLUMN_DEFAULT], [DATA_TYPE] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + tableName.Trim() + "'";
                result = ExecuteSql<string, string>(this.TranslatorSettings.SqlConnectionString, sql, convertDBSchema);
            }

            return result;
        }

        /// <summary>
        /// Translates the specified is full.
        /// </summary>
        public void Translate()
        {
            Translate(GetSelectedTables());
        }

        /// <summary>
        /// Translates the specified table names.
        /// </summary>
        /// <param name="tableNames">The table names.</param>
        /// <exception cref="System.Exception">Missing connection string.</exception>
        public void Translate(string[] tableNames)
        {
            if (HasSQLConnectionString && HasMongoDBConnectionString)
            {
                bool is2005OrAbove = true;

                try
                {
                    is2005OrAbove = Is2005OrAbove(GetSqlServerVersion(TestSqlConnection(this.TranslatorSettings.SqlConnectionString)));
                    if (!is2005OrAbove)
                    {
                        throw new Exception("Only SQL Server 2005 or above is supported.");
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }

                if (tableNames != null && tableNames.Length > 0)
                {
                    foreach (var mapping in this.TranslatorSettings.Mappings)
                    {
                        if (tableNames.Contains(mapping.SQLTable, StringComparer.InvariantCultureIgnoreCase))
                        {
                            mapping.CheckPageSizeAndOrderByColumn();
                            DoTranslate(mapping);
                        }
                    }
                }
            }
            else
            {
                throw new Exception("Missing connection string.");
            }
        }

        /// <summary>
        /// Does the translate.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="mongoCollection">The mongo collection.</param>
        /// <param name="startIdentity">The start identity.</param>
        /// <param name="pageSize">Size of the page.</param>
        protected void DoTranslate(TableMapping mapping)
        {
           // ProcessExecuter.ConsoleWriteLog("Translating [" + mapping.SQLTable.DBToString() + "] to [" + mapping.MongoCollection.DBToString() + "]...");
            int totalCount = 0;
            var schema = GetSchemaFromSQL(mapping.SQLTable);
            if (schema != null && schema.Count > 0)
            {
                bool toContinue = true;
                string syncIdentity = mapping.LastSyncIdentity;

                string primaryKeyColumn = string.IsNullOrWhiteSpace(mapping.PrimaryKeyColumn) ? schema.FirstOrDefault().Key : mapping.PrimaryKeyColumn;

                while (toContinue)
                {
                    string sqlText = GetSQLForSelectData(schema, primaryKeyColumn, mapping.PageSize, mapping.SQLTable, syncIdentity);
                    int increaseNumber = DoTranslate(sqlText, mapping.MongoCollection, primaryKeyColumn, mapping.GetConvertBsonDocumentDelegate(schema), ref syncIdentity);
                    toContinue = increaseNumber >= mapping.PageSize;
                    totalCount += increaseNumber;
                }
            }
            //Program.ConsoleWriteLine();
           // Program.ConsoleWriteLog("Total: " + totalCount.ToString());
        }

        /// <summary>
        /// Does the translate.
        /// </summary>
        /// <param name="sqlText">The SQL text.</param>
        /// <param name="mongoCollection">The mongo collection.</param>
        /// <param name="primaryKeyColumn">The primary key column.</param>
        /// <param name="convertBsonDocumentDelegate">The convert bson document delegate.</param>
        /// <param name="startIdentity">The start identity.</param>
        /// <returns></returns>
        protected int DoTranslate(string sqlText, string mongoCollection, string primaryKeyColumn, TableMapping.ConvertBsonDocumentDelegate convertBsonDocumentDelegate, ref string startIdentity)
        {
            List<BsonDocument> documents = new List<BsonDocument>();
            using (SqlConnection conn = new SqlConnection(this.TranslatorSettings.SqlConnectionString))
            {
                SqlCommand cmd = new SqlCommand(sqlText, conn);
                cmd.CommandType = CommandType.Text;
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }

                var mongoDB = GetMongoDB(this.TranslatorSettings.MongoConnectionString);
                var collectionOperator = mongoDB.GetCollection(mongoCollection);
                if (string.IsNullOrWhiteSpace(startIdentity))
                {
                    collectionOperator.RemoveAll();
                }

                try
                {
                    var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        startIdentity = reader[TableMapping.ColumnRowID].DBToString();
                        var bsonObj = convertBsonDocumentDelegate(reader, primaryKeyColumn);
                        documents.Add(bsonObj);
                    }

                    if (documents.Count > 0)
                    {
                        collectionOperator.InsertBatch(documents);
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }

            return documents.Count();
        }

        /// <summary>
        /// Gets the selected tables.
        /// </summary>
        /// <returns></returns>
        protected string[] GetSelectedTables()
        {
            List<string> result = new List<string>();
            foreach (var one in this.TranslatorSettings.Mappings)
            {
                if (one.IsSelected)
                {
                    result.Add(one.SQLTable);
                }
            }
            return result.ToArray();
        }

        /// <summary>
        /// Gets the SQL for truncate table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        protected string GetSQLForTruncateTable(string tableName)
        {
            return string.Format("Truncate table {0};", tableName);
        }

        /// <summary>
        /// Gets the SQL for truncate table.
        /// </summary>
        /// <param name="schema">The schema.</param>
        /// <param name="primaryName">Name of the primary.</param>
        /// <param name="pageSize">Size of the page.</param>
        /// <param name="table">The table.</param>
        /// <param name="lastSyncIdentity">The last sync identity.</param>
        /// <returns></returns>
        protected string GetSQLForSelectData(Dictionary<string, string> schema, string primaryName, int pageSize, string table, string lastSyncIdentity = null)
        {
            StringBuilder builder = new StringBuilder();

            if (schema != null && schema.Count > 0 && !string.IsNullOrWhiteSpace(table))
            {
                if (pageSize <= 0)
                {
                    pageSize = 500;
                }

                StringBuilder selectColumnsBuilder = new StringBuilder();
                foreach (var key in schema.Keys)
                {
                    selectColumnsBuilder.AppendFormat("[{0}],", key);
                }

                builder.Append("SELECT TOP " + pageSize.ToString() + " ");
                builder.Append(selectColumnsBuilder.ToString());
                builder.Append("[" + TableMapping.ColumnRowID + "]");

                builder.Append(" FROM ( SELECT ");
                builder.Append(selectColumnsBuilder.ToString());
                builder.Append("row_number() over(order by [" + primaryName + "]) AS " + TableMapping.ColumnRowID + " FROM dbo.[" + table.Trim() + "]");
               
                //Temp Code Start
                //string testcol = selectColumnsBuilder.ToString();
                //testcol = testcol.Remove(testcol.Length - 1);
                //builder.Append("row_number() over(order by " + testcol + ") AS " + TableMapping.ColumnRowID + " FROM dbo.[" + table.Trim() + "]");
                //Temp Code End
                
                builder.Append(") AS S");

                if (!string.IsNullOrWhiteSpace(lastSyncIdentity))
                {
                    builder.Append(" WHERE S.[" + TableMapping.ColumnRowID + "] > " + lastSyncIdentity);
                }

                builder.Append(";");
            }

            return builder.ToString();
        }

        #endregion

        #region SQL operations & utilities

        /// <summary>
        /// Executes the SQL.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="sqlText">The SQL text.</param>
        /// <returns></returns>
        public static List<T> ExecuteSql<T>(string connectionString, string sqlText, ConvertListEntityDelegate<T> converter)
        {
            List<T> list = new List<T>();

            if (converter != null)
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    SqlCommand cmd = new SqlCommand(sqlText, conn);
                    cmd.CommandType = CommandType.Text;
                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }

                    try
                    {
                        var reader = cmd.ExecuteReader();
                        list = converter(reader);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                    finally
                    {
                        if (conn != null && conn.State != ConnectionState.Closed)
                        {
                            conn.Close();
                        }
                    }
                }
            }

            return list;
        }

        /// <summary>
        /// Executes the SQL.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="sqlText">The SQL text.</param>
        /// <returns></returns>
        public static Dictionary<TKey, TValue> ExecuteSql<TKey, TValue>(string connectionString, string sqlText, ConvertDictionaryEntityDelegate<TKey, TValue> converter)
        {
            Dictionary<TKey, TValue> result = new Dictionary<TKey, TValue>();

            if (converter != null)
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    SqlCommand cmd = new SqlCommand(sqlText, conn);
                    cmd.CommandType = CommandType.Text;
                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }

                    try
                    {
                        var reader = cmd.ExecuteReader();
                        result = converter(reader);
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                    finally
                    {
                        if (conn != null && conn.State != ConnectionState.Closed)
                        {
                            conn.Close();
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Executes the SQL.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="sqlText">The SQL text.</param>
        /// <returns></returns>
        public static object ExecuteSqlScalar(string connectionString, string sqlText)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                SqlCommand cmd = new SqlCommand(sqlText, conn);

                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }

                try
                {
                    object result = cmd.ExecuteScalar();
                    return result;
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Tests the connection.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>Version string.</returns>
        public static string TestSqlConnection(string connectionString)
        {
            string result = null;
            string sqlText = "SELECT @@VERSION";
            try
            {
                result = (string)ExecuteSqlScalar(connectionString, sqlText);
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to test SQL connection for:" + connectionString.DBToString() + " caused by:" + ex.Message, ex);
            }

            return result;
        }

        /// <summary>
        /// Tests the connection.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>
        /// Size string.
        /// </returns>
        /// <exception cref="System.Exception">Failed to test Mongo connection for: + connectionString.DBToString() + , caused by: + ex.Message</exception>
        public static string TestMongoConnection(string connectionString)
        {
            string result = string.Empty;

            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                try
                {
                    var database = GetMongoDB(connectionString);
                    result = "Size: " + database.GetStats().DataSize.ToString();
                }
                catch (Exception ex)
                {
                    throw new Exception("Failed to test Mongo connection for:" + connectionString.DBToString() + ", caused by:" + ex.Message, ex);
                }
            }

            return result;
        }

        /// <summary>
        /// Gets the mongo DB.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">No database name found for Mongo DB.</exception>
        public static MongoDatabase GetMongoDB(string connectionString)
        {
            var mongoClient = new MongoClient(connectionString);
            var server = mongoClient.GetServer();
            string databaseName = string.Empty;

            /// mongodb://<dbuser>:<dbpassword>@ds059557.mongolab.com:59557/geography
            Uri newUrl = new Uri(connectionString.Replace("mongodb://", "http://"));
            databaseName = newUrl.Segments.LastOrDefault();

            if (!string.IsNullOrWhiteSpace(databaseName))
            {
                return server.GetDatabase(databaseName);
            }
            else
            {
                throw new Exception("No database name found for Mongo DB.");
            }
        }

        /// <summary>
        /// Gets the SQL server version.
        /// </summary>
        /// <param name="sqlConnectionString">The SQL connection string.</param>
        /// <returns></returns>
        public static string GetSqlServerVersion(string sqlConnectionString)
        {
            string version = string.Empty;
            Regex regex = new Regex(@"Microsoft(\s+)SQL(\s+)Server(\s+)(?<Version>(20([\w]+)))(\s+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var match = regex.Match(sqlConnectionString);
            if (match.Success)
            {
                version = match.Result("${Version}");
            }

            return version;
        }

        /// <summary>
        /// Is2008s the or above.
        /// </summary>
        /// <param name="version">The version.</param>
        /// <returns></returns>
        public static bool Is2005OrAbove(string version)
        {
            bool result = false;
            if (!string.IsNullOrWhiteSpace(version))
            {
                result = version.CompareTo("2004") > 0;
            }

            return result;
        }

        /// <summary>
        /// Gets the table names.
        /// </summary>
        /// <param name="sqlConnection">The SQL connection.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">Failed to get tables for: + sqlConnection.DBToString() + , caused by: + ex.Message</exception>
        public static List<TableMapping> GetTableNames(string sqlConnection)
        {
            List<TableMapping> result = new List<TableMapping>();
            //string sqlText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';";
            string sqlText = "SELECT  OBJECT_NAME(ic.OBJECT_ID) AS TableName,COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName,SUM(pa.rows) RowCnt FROM    sys.indexes AS i INNER JOIN sys.index_columns AS ic ON  i.OBJECT_ID = ic.OBJECT_ID AND i.index_id = ic.index_id INNER JOIN sys.partitions pa ON pa.OBJECT_ID = ic.OBJECT_ID WHERE   i.is_primary_key = 1 GROUP BY OBJECT_NAME(ic.OBJECT_ID),COL_NAME(ic.OBJECT_ID,ic.column_id);";
            try
            {
                ConvertListEntityDelegate<TableMapping> convertDelegate = new ConvertListEntityDelegate<TableMapping>(delegate(IDataReader reader)
                {
                    List<string> list = new List<string>();
                    List<TableMapping> listTableMapping = new List<TableMapping>();

                    if (reader != null)
                    {
                        while (reader.Read())
                        {
                            TableMapping tableMapping = new TableMapping();

                            tableMapping.SQLTable = reader["TableName"].DBToString();
                            tableMapping.MongoCollection = reader["TableName"].DBToString();
                            tableMapping.PrimaryKeyColumn = reader["ColumnName"].DBToString();
                            tableMapping.PageSize = Convert.ToInt32(reader["RowCnt"].DBToString());
                            tableMapping.IsSelected = true;

                            listTableMapping.Add(tableMapping);
                        }
                    }

                    return listTableMapping;
                });

                result = ExecuteSql(sqlConnection, sqlText, convertDelegate);

            }
            catch (Exception ex)
            {
                throw new Exception("Failed to get tables for:" + sqlConnection.DBToString() + ", caused by:" + ex.Message, ex);
            }

            return result;
        }

        #endregion
    }
}
