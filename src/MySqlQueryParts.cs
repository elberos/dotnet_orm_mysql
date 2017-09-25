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
		public string insert;
		public string delete;
		public string update;
		public string from;
		
		public MySqlQueryParts(){
			this.select_before = new List<string>();
			this.select = new List<string>();
			this.set = new List<string>();
			this.set_insert = new List<string>();
			this.set_update = new List<string>();
			
			this.where = "";
			this.insert = "";
			this.delete = "";
			this.update = "";
			this.from = "";
			this.order = "";
		}
	}
	
}