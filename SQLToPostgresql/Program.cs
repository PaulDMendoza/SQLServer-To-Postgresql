using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using System.Data.SqlClient;
using Dapper;

namespace SQLToPostgresql
{
    class Program
    {
        static void Main(string[] args)
        {
            var server = args.First(f => f.StartsWith("-server")).Replace("-server=", "");
            var db = args.First(f => f.StartsWith("-db")).Replace("-db=", "");

            StringBuilder sql = new StringBuilder();
//            sql.AppendLine(string.Format(@"CREATE DATABASE {0}
//  WITH OWNER = postgres
//       ENCODING = 'UTF8'
//       TABLESPACE = pg_default
//       LC_COLLATE = 'English_United States.1252'
//       LC_CTYPE = 'English_United States.1252'
//       CONNECTION LIMIT = -1;", db.ToLower()));

            var connectionStringBuilder = new SqlConnectionStringBuilder();
            connectionStringBuilder.DataSource = server;
            connectionStringBuilder.InitialCatalog = db;
            connectionStringBuilder.IntegratedSecurity = true;

            string connectionString = connectionStringBuilder.ConnectionString;

            using (var cn = new SqlConnection(connectionString))
            {
                var tables = cn.Query(@"select * from INFORMATION_SCHEMA.TABLES");
                var columns = cn.Query("select * from INFORMATION_SCHEMA.COLUMNS");
                var tableContraints = cn.Query(@"select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS");
                var referentialConstraints = cn.Query(@"select * from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS");
                var constraintsColumnUsage = cn.Query(@"select * from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE");
                var indexColumns = cn.Query(@"
SELECT 
     TableName = t.name,
     IndexName = ind.name,
     IndexId = ind.index_id,
     ColumnId = ic.index_column_id,
     ColumnName = col.name,
	 IsDesc = ic.is_descending_key,
    ind.is_primary_key
FROM 
     sys.indexes ind 
INNER JOIN 
     sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id 
INNER JOIN 
     sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id 
INNER JOIN 
     sys.tables t ON ind.object_id = t.object_id
");

                foreach (var table in tables)
                {
                    sql.AppendLine("CREATE TABLE " + table.TABLE_NAME.ToLower());
                    sql.AppendLine("(");
                    int columnOrdinal = 0;
                    foreach (var column in columns.Where(c => c.TABLE_NAME == table.TABLE_NAME))
                    {
                        if (column.DATA_TYPE == "geography")
                            continue; // This isn't a supported type in Postgresql.

                        if (columnOrdinal > 0)
                            sql.Append("\t,");
                        else
                            sql.Append("\t");

                         

                        sql.Append("\"" + column.COLUMN_NAME.ToLower() + "\" ");

                        

                        if (column.DATA_TYPE == "uniqueidentifier")
                        {
                            sql.Append("uuid");
                        }
                        else if (column.DATA_TYPE == "datetime")
                        {
                            sql.Append("timestamp");
                        }
                        else if (column.DATA_TYPE == "nvarchar" || column.DATA_TYPE == "varchar")
                        {
                            sql.Append("text");
                        }
                        else if (column.DATA_TYPE == "datetimeoffset" || column.DATA_TYPE == "smalldatetime")
                        {
                            sql.Append("timestamp");
                        }
                        else if (column.DATA_TYPE == "bit")
                        {
                            sql.Append("boolean");
                        }
                        else if (column.DATA_TYPE == "bigint")
                        {
                            sql.Append("bigint");
                        }
                        else if (column.DATA_TYPE == "date")
                        {
                            sql.Append("date");
                        }
                        else if (column.DATA_TYPE == "float")
                        {
                            sql.Append("double precision");
                        }
                        else if (column.DATA_TYPE == "real")
                        {
                            sql.Append("real");
                        }
                        else if (column.DATA_TYPE == "xml")
                        {
                            sql.Append("xml");
                        } else {
                            sql.Append(column.DATA_TYPE);
                        }

                        if (column.IS_NULLABLE == "NO")
                        {
                            sql.Append(" NOT NULL");
                        }

                        if (column.COLUMN_DEFAULT != null)
                        {
                            // This is hardcoded to my implementation for my data.
                            if (column.DATA_TYPE == "bit" && column.COLUMN_DEFAULT == "((0))")
                            {
                                sql.Append(" DEFAULT FALSE");
                            }
                            else if (column.DATA_TYPE == "bit" && column.COLUMN_DEFAULT == "((1))")
                            {
                                sql.Append(" DEFAULT TRUE");
                            }
                            else if(column.DATA_TYPE == "int")
                            {
                                sql.Append(" DEFAULT " +  column.COLUMN_DEFAULT.Replace("(", "").Replace(")", ""));
                            }
                            else if (column.COLUMN_DEFAULT == "(getutcdate())" || column.DATA_TYPE == "datetimeoffset" || column.DATA_TYPE == "datetime")
                            {
                                sql.Append(" DEFAULT timezone('utc'::text, now())");
                            }
                        }

                        sql.AppendLine();
                        columnOrdinal++;

                    }
                    sql.AppendLine(") WITH ( OIDS=FALSE);");

                    sql.AppendLine();
                }

                

                foreach (var tableConstraint in tableContraints.Where(tc=>!String.IsNullOrWhiteSpace(tc.TABLE_NAME)))
                {
                    if (tableConstraint.CONSTRAINT_TYPE == "PRIMARY KEY")
                    {  
                        var pkColumnObjs = indexColumns.Where(c => c.IndexName == tableConstraint.CONSTRAINT_NAME).Select(s => new { COLUMN_NAME = s.ColumnName, ORDER = s.ColumnId });
                        var pkColumns = string.Join(",", pkColumnObjs.OrderBy(s => s.ORDER).Select(s => s.COLUMN_NAME));
                        string primaryKey = string.Format(@"ALTER TABLE {2}
  ADD CONSTRAINT {0} PRIMARY KEY({1});", tableConstraint.CONSTRAINT_NAME.ToString().ToLower().Replace("[", "").Replace("]", ""), pkColumns.ToLower(), tableConstraint.TABLE_NAME.ToLower());
                        sql.AppendLine(primaryKey);
                    }
                }

                var keyColumnUsage = cn.Query(@"select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE");
                
                var foreignKeys = cn.Query(@"select ctu.TABLE_NAME, rc.CONSTRAINT_NAME, uniqueTableConstraint.CONSTRAINT_NAME as PrimaryKeyConstraintName,  uniqueTableConstraint.TABLE_NAME as ReferencedTableName from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
inner join INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu on ctu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS uniqueTableConstraint on uniqueTableConstraint.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME");

                foreach (var fk in foreignKeys)
                {
                    var keyColumnsCSV = string.Join(",", keyColumnUsage.Where(c => c.CONSTRAINT_NAME == fk.CONSTRAINT_NAME).OrderBy(c => c.ORDINAL_POSITION).Select(c => c.COLUMN_NAME));
                    var primaryKeyColumns = indexColumns.Where(ic => ic.IndexName == fk.PrimaryKeyConstraintName);

                    var constraintColumnsCSV = string.Join(",", primaryKeyColumns.Select(c=>c.ColumnName));

                    var joinedToTable = primaryKeyColumns.First().TableName;

                    string fkSQL = string.Format(@"ALTER TABLE {0}
  ADD CONSTRAINT {1} FOREIGN KEY ({2})
      REFERENCES {3} ({4}) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION;", fk.TABLE_NAME.ToLower(), fk.CONSTRAINT_NAME.ToString().ToLower(), keyColumnsCSV, joinedToTable.ToLower(), constraintColumnsCSV);
                    sql.AppendLine(fkSQL);
                }



                foreach (var index in indexColumns.Where(ic => !ic.is_primary_key).GroupBy(g => g.IndexName))
                {
                    var indexColumnsInIndex = index.Select(s => new { COLUMN_NAME = s.ColumnName, ORDER = s.ColumnId });
                    var indexColumnCsv = string.Join(",", indexColumnsInIndex.OrderBy(s => s.ORDER).Select(s => s.COLUMN_NAME));
                    var first = index.First();

                    string indexSQL = string.Format(@"
CREATE INDEX {0}
  ON {1}
  USING btree
  ({2});", first.IndexName.ToLower(), first.TableName.ToLower(), indexColumnCsv.ToLower() );
                    sql.AppendLine(indexSQL);
                }
            }


            // OUTPUT SQL FILE AND OPEN
            File.WriteAllText("output.txt", sql.ToString());

            Process.Start("output.txt");


        }
    }
}
