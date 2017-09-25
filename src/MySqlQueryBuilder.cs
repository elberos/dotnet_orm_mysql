/**
 * This file is part of the Elberos package.
 *
 * (c) Ildar Bikmamatov <elberos@bayrell.org>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Data.Common;
using MySql.Data;
using MySql.Data.MySqlClient;
using Elberos.Orm;

namespace Elberos.Orm.MySql{
	
	public class MySqlQueryBuilder : QueryBuilder {
		
		static readonly string[] _FILTER_TYPES = new string[] {
				"boolean", "longeger", "double", "string", "NULL"
			};
		
		
		/*
		https://stackoverflow.com/questions/186588/which-is-fastest-select-sql-calc-found-rows-from-table-or-select-count
		*/
		public static readonly long FOUND_ROWS_NONE = 0;
		
		// Get found rows by SQL_CALC_FOUND_ROWS
		public static readonly long FOUND_ROWS_CALC = 1;
		
		// Get found rows by two query
		public static readonly long FOUND_ROWS_QUERY = 2;
		
		protected long _found_rows_type = MySqlQueryBuilder.FOUND_ROWS_CALC;
		
		
		/**
		 * Parts of the query
		 */
		protected MySqlQueryParts _sql_parts = null;
		protected List<QueryParameter> _parameters = null;
		protected long _params_inc = 0;
		protected string _build_sql = null;
		
		// Commands 
		protected MySqlCommand _cmd_found_rows = null;
		protected MySqlCommand _cmd_query = null;
		
		// Command Result
		protected DbDataReader _cmd_result = null;
		
		
		/**
		 * Generate next parameter
		 */
		protected string getNextParameter(){
			return "p_" + this._params_inc++;
		}
		
		
		
		/**
		 * Get field name as "alias.field_name"
		 *
		 * @param string field_name The field_name
		 * @return string field_name with synonym
		 */
		public override string getFieldName(string field_name){
			
			if (MySqlConnection.MYSQL_RESERVERD_WORDS.IndexOf(field_name) > -1)
				field_name = "`" + field_name + "`";
			
			if (!this.canAlias())
				return field_name;
			
			if (field_name.IndexOf(".") == -1)
				return this._alias + "." + field_name;
			
			return field_name;
		}
		
		
		
		/**
		 * Set sql parameter
		 *
		 * @param string name
		 * @param mixed value
		 * @param string|null type
		 * @return self
		 */
		public QueryBuilder setParameter(string name, dynamic value){
			this._parameters.Add(new QueryParameter(name, value));
			return this;
		}
		
		
		
		/**
		 * Return sql parameters
		 *
		 * @param List<Parameter> 
		 */
		public List<QueryParameter> getParameters(){
			return this._parameters;
		}
		
		
		
		/**
		 * Get sql part
		 *
		 * @return Dictionary<string, dynamic>
		 */
		public MySqlQueryParts getSqlQuery(){
			return this._sql_parts;
		}
		


		/**
		 * Set sql part
		 *
		 * @param Dictionary<string, dynamic> value
		 * @return self
		 */
		public QueryBuilder setSqlQuery(MySqlQueryParts value){
			this._sql_parts = value;
			return this;
		}
		
		
		
		/**
		 * Build select query
		 *
		 * @param array fields List of the fields
		 * @return self
		 */
		public override QueryBuilder select(string[] fields = null){
			this._query_type = MySqlQueryBuilder.QUERY_SELECT;
			
			foreach (string field in fields){
				this._sql_parts.select.Add( this.getFieldName(field) );
			}
			
			return this;
		}
		
		
		
		/**
		 * Build insert query
		 *
		 * @param string entity_name 
		 * @param string alias 
		 * @return self
		 */
		public override QueryBuilder insert(string entity_name, string alias = null){
			this._query_type = MySqlQueryBuilder.QUERY_INSERT;
			this.setEntity(entity_name, alias);
			this._sql_parts.insert = this.getTableName();
			return this;
		}
		
		
		
		/**
		 * Build update query
		 *
		 * @param string entity_name 
		 * @param string alias 
		 * @return self
		 */
		public override QueryBuilder update(string entity_name, string alias = null){
			this._query_type = MySqlQueryBuilder.QUERY_UPDATE;
			this.setEntity(entity_name, alias);
			this._sql_parts.update = this.getTableName() + " " + this._alias;
			return this;
		}
		
		
		
		/**
		 * Build insert or update query
		 *
		 * @param string entity_name 
		 * @param string alias 
		 * @return self
		 */
		public override QueryBuilder insertOrUpdate(string entity_name, string alias = null){
			this._query_type = MySqlQueryBuilder.QUERY_INSERT_OR_UPDATE;
			this.setEntity(entity_name, alias);
			this._sql_parts.insert = this.getTableName();
			return this;
		}
		
		
		
		/**
		 * Build delete query
		 *
		 * @param string entity_name 
		 * @param string alias 
		 * @return self
		 */
		public override QueryBuilder delete(string entity_name, string alias = null){
			this._query_type = MySqlQueryBuilder.QUERY_DELETE;
			this.setEntity(entity_name, alias);
			return this;
		}
		
		
		
		/**
		 * Set where sql part
		 *
		 * @param string where
		 * @return self
		 */
		public QueryBuilder where(string where){
			this._sql_parts.where = where;
			return this;
	    }
		
		
		
		/**
	     * Add field to select query
	     *
	     * @param string field The field
	     * @return self
	     */
	    public override QueryBuilder addSelect(string field = null){
			this._sql_parts.select.Add(field);
			return this;
		}
		
		
		
		/**
	     * Adds a distinct flag of the select query
		 *
		 * @param bool flag
		 * @return self
	     */
	    public override QueryBuilder distinct(bool flag = true){
			if (flag){
				if (this._sql_parts.select_before.IndexOf("DISTINCT") == -1){
					this._sql_parts.select_before.Add("DISTINCT");
				}
			}
			else{
				if (this._sql_parts.select_before.IndexOf("DISTINCT") > -1){
					this._sql_parts.select_before.Remove("DISTINCT");
				}
			}
	        return this;
		}
		
		
		
		/**
	     * Adds a SQL_CALC_FOUND_ROWS flag of the select query
		 *
		 * @param bool flag
		 * @return self
	     */
	    public override QueryBuilder calcFoundRows(bool flag = true){
			if (this._found_rows_type != MySqlQueryBuilder.FOUND_ROWS_CALC)
				return this;
			
			if (flag){
				if (this._sql_parts.select_before.IndexOf("SQL_CALC_FOUND_ROWS") == -1){
					this._sql_parts.select_before.Add("SQL_CALC_FOUND_ROWS");
				}
			}
			else{
				if (this._sql_parts.select_before.IndexOf("SQL_CALC_FOUND_ROWS") == -1){
					this._sql_parts.select_before.Remove("SQL_CALC_FOUND_ROWS");
				}
			}
			
	        return this;
		}
		
		
		
		/**
		 * Set new field to query
		 *
		 * @param string key The key
	     * @param dynamic value The value
		 * @return self
		 */
		public override QueryBuilder set(string key, dynamic value){
			if (value == null){
				this._sql_parts.set[key] = null;
				return this;
			}
			
			string p = this.getNextParameter();
			this.setParameter(p, value);			
			this._sql_parts.set[key] = "@" + p;
			
			return this;
		}
		
		
		
		/**
		 * Set new field to insert query
		 *
		 * @param string key The key
	     * @param dynamic value The value
		 * @return self
		 */
		public override QueryBuilder setInsert(string key, dynamic value){
			if (value == null){
				this._sql_parts.set_insert[key] = null;
				return this;
			}
			
			string p = this.getNextParameter();
			this.setParameter(p, value);			
			this._sql_parts.set_insert[key] = "@" + p;
			
			return this;
		}
		
		
		
		/**
		 * Set new field to update query
		 *
		 * @param string key The key
	     * @param dynamic value The value
		 * @return self
		 */
		public override QueryBuilder setUpdate(string key, dynamic value){
			if (value == null){
				this._sql_parts.set_update[key] = null;
				return this;
			}
			
			string p = this.getNextParameter();
			this.setParameter(p, value);			
			this._sql_parts.set_update[key] = "@" + p;
			
			return this;
		}
		
		
		
		/**
		 * Set new raw field to query
		 *
		 * @param string key The key
	     * @param dynamic value The value
		 * @return self
		 */
		public override QueryBuilder setRaw(string key, dynamic value){
			if (value == null){
				this._sql_parts.set[key] = null;
				return this;
			}
			
			this._sql_parts.set[key] = value;
			
			return this;
		}
		
		
		
		/**
		 * Set new raw field to insert query
		 *
		 * @param string key The key
	     * @param dynamic value The value
		 * @return self
		 */
		public override QueryBuilder setInsertRaw(string key, dynamic value){
			if (value == null){
				this._sql_parts.set_insert[key] = null;
				return this;
			}
			
			this._sql_parts.set_insert[key] = value;
			
			return this;
		}
		
		
		
		/**
		 * Set new raw field to update query
		 *
		 * @param string key The key
	     * @param dynamic value The value
		 * @return self
		 */
		public override QueryBuilder setUpdateRaw(string key, dynamic value){
			if (value == null){
				this._sql_parts.set_update[key] = null;
				return this;
			}
			
			this._sql_parts.set_update[key] = value;
			
			return this;
		}
		
		
		
		/**
		 * Set new values
		 *
		 * @param array arr The array of key => value
		 * @return self
		 */
		public QueryBuilder setValues(Dictionary<string, dynamic> arr){
			foreach (KeyValuePair<string, dynamic> entry in arr)
				this.set(entry.Key, entry.Value);
				
			return this;
		}
		
		
		
		/**
		 * Set order
		 *
		 * @param array arr Order list
		 * @return self
		 */
		public QueryBuilder order(Dictionary<string, long> arr){
			
			List<string> list = new List<string>();
			foreach (KeyValuePair<string, long> entry in arr){
				if (entry.Value == QueryOrder.ASC){
					list.Add(entry.Key + " ASC");
				}
				else if (entry.Value == QueryOrder.DESC){
					list.Add(entry.Key + " DESC");
				}
			}
				
			this._sql_parts.order = list;
			return this;
		}
		
		
		
		/**
		 * Run query and get data
		 *
		 * @return self
		 */
		public override async Task<QueryBuilder> execute(){
			MySqlConnection connection = (MySqlConnection)this.getConnection();
			
			// Create found rows query
			if (this._found_rows_type == MySqlQueryBuilder.FOUND_ROWS_QUERY && 
					this._query_type == MySqlQueryBuilder.QUERY_SELECT){
				
				/*
				// Get query
				qb = clone this;
				qb.limit(-1);
				qb.setSQL('SELECT', ["COUNT(*)"], false);
				
				this._found_rows_query = qb.prepare();
				 */
			}
			
			else if (this._found_rows_type == MySqlQueryBuilder.FOUND_ROWS_CALC && 
					this._query_type == MySqlQueryBuilder.QUERY_SELECT){
				
				// Get query
				this._cmd_found_rows = connection.createCommand("SELECT FOUND_ROWS()");
				
			}
			else
				this._cmd_found_rows = null;
			
			
			// Execute query
			this._found_rows = -1;
			this._query_result = null;
			this._cmd_query = this.prepare();
			this._cmd_result = null;
			
			if (this._query_type == MySqlQueryBuilder.QUERY_SELECT){
				this._cmd_result = await this._cmd_query.ExecuteReaderAsync();
			}
			else{
				await this._cmd_query.ExecuteNonQueryAsync();
			}
			
			return this;
		}
		
		
		
		/**
		 * Return raw result
		 *
		 * @return Statement
		 */
		public override DbDataReader getRawResult(){
			return this._cmd_result;
		}
		
		
		
		/**
	     * Get found rows
	     *
	     * @return long 
	     */
	    public override long foundRows(){
			if (this._found_rows == -1 && this._cmd_found_rows != null){
				this._cmd_found_rows.ExecuteScalarAsync();
				//this._found_rows = this._found_rows_query.fetchColumn(0);
	        }
	        return this._found_rows;
		}
		
		
		
		/**
	     * Get last insert id
	     *
	     * @return long 
	     */
	    public override dynamic lastInsertId(){
			return 0;
			//return this.getConnection().lastInsertId();
		}
		
		
		
		/**
		 * Prepare SQL
		 *
		 * @return Doctrine\DBAL\Statement
		 */
		protected MySqlCommand prepare(){
			
			MySqlConnection connection = (MySqlConnection)this.getConnection();
			if (!connection.connected())
				connection.connect();
			
			// Create SQL
			string sql = this.buildSQL();
			
			// Prepare SQL
			MySqlCommand cmd = connection.createCommand(sql);
			
			// Add params
			List<QueryParameter> arr = this.getParameters();
			foreach (QueryParameter param in arr){
				cmd.Parameters.AddWithValue(param.key, param.value);
			}
			
			return cmd;
		}
		
		
		
		/**
		 * Get raw SQL query
		 *
		 * @return string
		 */
		public string getSql(){
			Connection connection = this.getConnection();
			
			string sql = this.buildSQL();
			List<QueryParameter> arr = this.getParameters();
			
			foreach(QueryParameter param in arr){
				string key = param.key;
				string value = param.value;
				
				Regex rgx = new Regex(":"+key, RegexOptions.IgnoreCase);
				
				if (value == null) value = "null";
				else if (value == "") value = "''";
				else value = connection.escapeVar(value);
				
				rgx.Replace(sql, value);
			}
			
			return sql;
		}
		
		
		
		/**
		 * Get SQL
		 *
		 * @return string
		 */
		protected string buildSQL(){
			
			if (this._build_sql != null)
				return this._build_sql;
			
			List<string> sql = new List<string>();
			
			if (this._filter.Count > 0 && this._sql_parts.where == ""){
				List<string> and_where = this.filterRecurse(this._filter);
				string where = String.Join(" and ", and_where);
				
				// Set where and params
				if (where != "")
					this._sql_parts.where = where;
			}
			
			if (this._query_type == MySqlQueryBuilder.QUERY_SELECT){
				sql.Add("SELECT");
				
				if (this._sql_parts.select_before.Count > 0)
					sql.Add( String.Join(" ", this._sql_parts.select_before) );
				
				if (this._sql_parts.select.Count > 0)
					sql.Add( String.Join(", ", this._sql_parts.select) );
				
				sql.Add("FROM " + this._sql_parts.from);
			}
			else if (this._query_type == MySqlQueryBuilder.QUERY_INSERT || 
					this._query_type == MySqlQueryBuilder.QUERY_INSERT_OR_UPDATE){
				sql.Add("INSERT INTO " + this._sql_parts.insert);
			}
			else if (this._query_type == MySqlQueryBuilder.QUERY_UPDATE){
				sql.Add("UPDATE " + this._sql_parts.update);
			}
			else if (this._query_type == MySqlQueryBuilder.QUERY_DELETE){
				sql.Add("DELETE FROM " + this._sql_parts.from);
			}
			
			if (this._query_type == MySqlQueryBuilder.QUERY_INSERT || this._query_type == MySqlQueryBuilder.QUERY_INSERT_OR_UPDATE){
				List<string> arr = new List<string> ();
				
				if (this._sql_parts.set_insert.Count > 0){
					foreach (KeyValuePair<string, string> entry in this._sql_parts.set_insert){
						arr.Add(entry.Key + " = " + entry.Value);
					}
				}
				if (this._sql_parts.set.Count > 0){
					foreach (KeyValuePair<string, string> entry in this._sql_parts.set){
						arr.Add(entry.Key + " = " + entry.Value);
					}
				}
				
				if (arr.Count > 0)
					sql.Add("SET " + String.Join(", ", arr));
			}
			
			else if (this._query_type == MySqlQueryBuilder.QUERY_UPDATE){
				List<string> arr = new List<string> ();
				
				if (this._sql_parts.set.Count > 0){
					foreach (KeyValuePair<string, string> entry in this._sql_parts.set){
						arr.Add(entry.Key + " = " + entry.Value);
					}
				}
				if (this._sql_parts.set_update.Count > 0){
					foreach (KeyValuePair<string, string> entry in this._sql_parts.set_update){
						arr.Add(entry.Key + " = " + entry.Value);
					}
				}
				if (arr.Count > 0)
					sql.Add("SET " + String.Join(", ", arr));
			}
			
			
			if (this._query_type == MySqlQueryBuilder.QUERY_INSERT_OR_UPDATE){
				List<string> arr = new List<string> ();
				
				if (this._sql_parts.set.Count > 0){
					foreach (KeyValuePair<string, string> entry in this._sql_parts.set){
						arr.Add(entry.Key + " = " + entry.Value);
					}
				}
				if (this._sql_parts.set_update.Count > 0){
					foreach (KeyValuePair<string, string> entry in this._sql_parts.set_update){
						arr.Add(entry.Key + " = " + entry.Value);
					}
				}
				if (arr.Count > 0)
					sql.Add("ON DUPLICATE KEY UPDATE " + String.Join(", ", arr));
			}
			
			if (this._sql_parts.where != "")
				sql.Add("WHERE " + this._sql_parts.where);
			
			
			if (this._sql_parts.order.Count > 0)
				sql.Add("ORDER BY " + String.Join(", ", this._sql_parts.order));
			
			
			long start = this.getStart();
			if (start > 0){
				sql.Add("OFFSET " + start);
			}
				
			long limit = this.getLimit();
			if (limit >= 0){
				sql.Add("LIMIT " + limit);
			}	
			
			this._build_sql = String.Join(" ", sql);
			return this._build_sql;
		}
		
		
		
		/**
		 * Run query and get data
		 */
		protected List<string> filterRecurse(List<QueryFilter> filter){
			List<string> where = new List<string>();
			
			foreach (QueryFilter arr in filter){
				
				// Add "or" operator
				if (arr.type == QueryFilter.TYPE_OR && arr.value is List<QueryFilter>){
					List<string> or_where = this.filterRecurse(arr.value);
					where.Add("(" + String.Join(" or ", or_where) + ")");
				}
				
				// Add "and" operator
				else if (arr.type == QueryFilter.TYPE_AND && arr.value is List<QueryFilter>){
					List<string> and_where = this.filterRecurse(arr.value);
					where.Add("(" + String.Join(" and ", and_where) + ")");
				}
								
				
				// Add operator
				else{
					
					// Add null operator
					if (arr.value == null){
						if (arr.type == QueryFilter.TYPE_EQUAL){
							where.Add(this.getFieldName(arr.key) + " is null");
						}
						else if (arr.type == QueryFilter.TYPE_NOT_EQUAL){
							where.Add(this.getFieldName(arr.key) + " is not null");
						}
						else{
							where.Add("1 = 0");
						}
					}
					
					// Add in_array operator
					else if (arr.type == QueryFilter.TYPE_IN){
						if (arr.value is List<dynamic>){
							List<string> r = new List<string>();
							foreach (dynamic item in arr.value){
								/*
								type = gettype(item);
								if (!in_array(type, MySqlQueryBuilder._FILTER_TYPES)){
									throw new \Exception("Wrong type '".type."' of the field " . field_name);
								}
								 */
								string p = this.getNextParameter();
								this.setParameter(p, item);
								r.Add("@" + p);
							}
							
							where.Add(this.getFieldName(arr.key) + " in [" + String.Join(", ", r) + "]");
						}
						else{
							where.Add("1 = 0");
						}
					}
					
					
					// Add other operator
					else{
						where.Add(this.filterOperation(arr));
					}
				}
				
			}
			
			return where;
		}
		
		
		
		/**
		 * Get filter operation
		 */
		protected string filterOperation(QueryFilter filter){
			/*
			type = gettype(val);
			if (!in_array(type, MySqlQueryBuilder._FILTER_TYPES)){
				throw new \Exception("Wrong type '".type."' of the field " . field_name);
			}
			 */
			string p = this.getNextParameter();
			this.setParameter(p, filter.value);
			
			return this.getFieldName(filter.key) + " " + filter.type + " :" + p;
		}
		
		

	}
}