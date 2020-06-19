using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace SqlToMongoCore
{
    public static class Extensions
    {
        /// <summary>
        /// DBs to string.
        /// </summary>
        /// <param name="obj">The obj.</param>
        /// <returns></returns>
        public static string DBToString(this object obj)
        {
            return obj == null ? string.Empty : obj.ToString();
        }

        /// <summary>
        /// Creates the XML node.
        /// </summary>
        /// <param name="obj">The obj.</param>
        /// <param name="nodeName">Name of the node.</param>
        /// <returns></returns>
        public static XElement CreateXmlNode(this object obj, string nodeName)
        {
            return XElement.Parse(string.Format("<{0}></{0}>", string.IsNullOrWhiteSpace(nodeName) ? "Item" : nodeName.Trim()));
        }

        /// <summary>
        /// Gets the attribute value.
        /// </summary>
        /// <param name="obj">The obj.</param>
        /// <param name="attributeName">Name of the attribute.</param>
        /// <returns></returns>
        public static string GetAttributeValue(this XElement obj, string attributeName)
        {
            XAttribute attribute = (obj != null && !string.IsNullOrWhiteSpace(attributeName)) ? obj.Attribute(attributeName) : null;

            return attribute != null ? attribute.Value : string.Empty;
        }

        /// <summary>
        /// Encodes the base64 from string.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        public static string EncodeBase64(this string source)
        {
            try
            {
                byte[] bytes = System.Text.Encoding.Default.GetBytes(source);
                return Convert.ToBase64String(bytes);
            }
            catch
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Decodes the base64 to string.
        /// </summary>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        public static string DecodeBase64(this string result)
        {
            try
            {
                byte[] bytes = Convert.FromBase64String(result);
                return System.Text.Encoding.Default.GetString(bytes);
            }
            catch
            {
                return string.Empty;
            }
        }
    }
}
