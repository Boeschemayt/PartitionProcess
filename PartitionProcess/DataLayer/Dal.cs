using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PartitionProcess.DataLayer
{
    class Dal
    {
        public string FullInstall() {
            string returnmessage = "";

            DLInstall dInstall = new DLInstall();

            try
            {
                //procedure
               returnmessage = returnmessage +"; "+ dInstall.CreateProcedureInsertTabularPartitionExisting();
               returnmessage = returnmessage +"; "+ dInstall.CreateProcedureInsertTabularPartitionRequired();
               returnmessage = returnmessage +"; "+ dInstall.CreateProcedureTruncateTablePartitionsTables();
               returnmessage = returnmessage + "; " + dInstall.CreateProcedureUpdateTabularPartitionsRequiredProcessState();

                //tables
                returnmessage =  returnmessage + "; " + dInstall.CreateTabularPartitionConfig();
                returnmessage =  returnmessage + "; " + dInstall.CreateTabularPartitionDatum();
                returnmessage = returnmessage + "; " + dInstall.CreateTabularPartitionExisting();
                returnmessage = returnmessage + "; " + dInstall.CreateTabularPartitionGrainMapping();
                returnmessage = returnmessage + "; " + dInstall.CreateTabularPartitionRequired();
                returnmessage = returnmessage + "; " + dInstall.CreateTabularPartitionvDatumPart();
            }
            catch (Exception)
            {
                returnmessage = "Install not complete.";
                throw;
            }
            return returnmessage;
        }
    }
}
