using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;

namespace PartitionProcess.DataLayer
{
    
    class DLInstall
    {
        private string _ConnectionString { get; set; }

        public DLInstall(string ConnectionString) {

            _ConnectionString = ConnectionString;
        }
        public DLInstall() {
            _ConnectionString = "Data Source=DESKTOP-PIPOOQN\\SQLEXPRESS;Integrated Security=SSPI;Initial Catalog=Mart";
        }

        public string CreateTabularPartitionConfig() {
            SqlConnection conn = new SqlConnection(_ConnectionString);
            string returnText = "";
            string fileName = "Create_Tabular_Partition_Config.sql";

            string dir = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName.ToString();
            var query = File.ReadAllText(dir + @"\Scripts\SQL Create scripts\" +fileName);

            
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, conn))
                {

                    conn.Open();
                    cmd.ExecuteNonQuery();
                    conn.Close();

                };
            }
            catch (SqlException e)
            {
                returnText = "Table TabularPartitionConfig was not installed.. " + e.ToString();
                throw new Exception(e.ToString()); //debug
            }

            returnText = "Table TabularPartitionConfig was successfully installed.";
            return returnText;
            
        }

        
    }
}
