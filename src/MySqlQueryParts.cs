/**
 * This file is part of the Elberos package.
 *
 * (c) Ildar Bikmamatov <elberos@bayrell.org>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using Elberos.Orm;

namespace Elberos.Orm.MySql{
	
	public class MySqlQueryParts{
		
		public List<string> select_before = null;
		public List<string> select = null;
		public Dictionary<string, string> set = null;
		public Dictionary<string, string> set_insert = null;
		public Dictionary<string, string> set_update = null;
		public List<string> order = null;
		
		public string where;
		public string table_name;
		
		public MySqlQueryParts(){
			this.select_before = new List<string>();
			this.select = new List<string>();
			this.set = new Dictionary<string, string>();
			this.set_insert = new Dictionary<string, string>();
			this.set_update = new Dictionary<string, string>();
			this.order = new List<string>();
			
			this.where = "";
			this.table_name = "";
		}
	}
	
}