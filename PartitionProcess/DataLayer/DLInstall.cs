using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace PartitionProcess.DataLayer
{
    
    class DLInstall
    {
        private string _ConnectionString { get; set; }

        public DLInstall(string ConnectionString) {

            _ConnectionString = ConnectionString;
        }

        public void CreateTabularPartitionConfig() {
            SqlConnection conn = new SqlConnection(_ConnectionString);

            string query = "CREATE TABLE [partitioning].[Tabular_Partition_Config]( " +
                            "[Tabular_Partition_Config_Id][int] IDENTITY(1, 1) NOT NULL," +
                            "[Tabular_Database_Name] [varchar] (100) NULL," +
                            "[Table_Name] [varchar] (100) NULL," +
                            "[Partition_Name_Prefix] [varchar] (100) NULL," +
                            "[Partitioning_Column] [varchar] (100) NULL," +
                            "[Source_Object] [varchar] (100) NULL," +
                            "[Stage_Table] [varchar] (100) NULL," +
                            "[Stage_Column] [varchar] (100) NULL," +
                            "[Partition_Grain] [varchar] (100) NULL," +
                            "[Partitions_Start_Date_SK] [int] NULL," +
                            "[Partitions_End_Date_SK] [int] NULL," +
                            "[Partitions_Start_Date] [date] NULL," +
                            "[Partitions_End_Date] [date] NULL," +
                            "[Process_Grain] [int] NULL" +
                            ")";
                            
            using (SqlCommand cmd = new SqlCommand(query, conn)) {

                conn.Open();
                cmd.ExecuteNonQuery();
                conn.Close();
            };
        }
    }
}
