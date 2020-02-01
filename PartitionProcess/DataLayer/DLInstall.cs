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

        /// <summary>
        /// Create table Tabular_Partition_Config in database.
        /// </summary>
        /// <returns></returns>
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
        /// <summary>
        /// Create table Tabular_Partition_Existing in database.
        /// </summary>
        /// <returns></returns>
        public string CreateTabularPartitionExisting()
        {
            SqlConnection conn = new SqlConnection(_ConnectionString);
            string returnText = "";
            string fileName = "Create_Tabular_Partition_Existing.sql";

            string dir = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName.ToString();
            var query = File.ReadAllText(dir + @"\Scripts\SQL Create scripts\" + fileName);


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
                returnText = "Table TabularPartitionExisting was not installed.. " + e.ToString();
                throw new Exception(e.ToString()); //debug
            }

            returnText = "Table TabularPartitionExisting was successfully installed.";
            return returnText;

        }
        /// <summary>
        /// Create table Tabular_Partition_Grain_Mapping in database.
        /// </summary>
        /// <returns></returns>
        public string CreateTabularPartitionGrainMapping()
        {
            SqlConnection conn = new SqlConnection(_ConnectionString);
            string returnText = "";
            string fileName = "Create_Tabular_Partition_Grain_Mapping.sql";

            string dir = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName.ToString();
            var query = File.ReadAllText(dir + @"\Scripts\SQL Create scripts\" + fileName);


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
                returnText = "Table TabularPartitionGrainMapping was not installed.. " + e.ToString();
                throw new Exception(e.ToString()); //debug
            }

            returnText = "Table TabularPartitionGrainMapping was successfully installed.";
            return returnText;

        }
        /// <summary>
        /// Create table Tabular_Partition_Required in database.
        /// </summary>
        /// <returns></returns>
        public string CreateTabularPartitionRequired()
        {
            SqlConnection conn = new SqlConnection(_ConnectionString);
            string returnText = "";
            string fileName = "Create_Tabular_Partitions_Required.sql";

            string dir = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName.ToString();
            var query = File.ReadAllText(dir + @"\Scripts\SQL Create scripts\" + fileName);


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
                returnText = "Table TabularPartitionRequired was not installed.. " + e.ToString();
                throw new Exception(e.ToString()); //debug
            }

            returnText = "Table TabularPartitionRequired was successfully installed.";
            return returnText;

        }

        /// <summary>
        /// Create table Datum in database.
        /// </summary>
        /// <returns></returns>
        public string CreateTabularPartitionDatum()
        {
            SqlConnection conn = new SqlConnection(_ConnectionString);
            string returnText = "";
            string fileName = "Create_Datum.sql";

            string dir = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName.ToString();
            var query = File.ReadAllText(dir + @"\Scripts\SQL Create scripts\" + fileName);


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
                returnText = "Table Datum was not installed.. " + e.ToString();
                throw new Exception(e.ToString()); //debug
            }

            returnText = "Table Datum was successfully installed.";
            return returnText;

        }

    }
}
