using System;
using System.Text;
using System.Configuration;
using System.Threading;
using System.IO;

namespace suptass
{
    class suptass
    {
        private static string connstr, execstr, logfile;
        private static bool writelog;
        private static int insts, tottime, secs;
        private static DateTime finishtime;
        private static FileStream fs;

        // to synchronize access in QueryDb method
        private static Object objLock = new Object();
        public static Object objLogLock = new Object();

        private static long threadcount = 0;
        private static long numruns = 0;
        private static double tot_resp_time = 0;
        private static double max_resp_time = 0;
        private static double min_resp_time = 0;
        
        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("* Suptass v1.0 - Simple Utility for Performance Testing Against SQL Server\n" +
                                  "| Usage:\n" + 
                                  "|       suptass.exe <# of clients> <run length (mins)> <call interval (secs)>\n" +
                                  "| Example:\n" +
                                  "|       C:\\>suptass.exe 10 30 1\n" +
                                  "|\n" +
                                  "| This example uses 10 different client connections, ends after 30 minutes,\n" +
                                  "| with approximately 1 second between each database call. All command line\n" +
                                  "| parameters are required and must be numeric. SQL Server connection details\n" +
                                  "| and the SQL statement to execute for stress testing are stored in the\n" +
                                  "| configuration file \"suptass.exe.config\".\n" +
                                  "|\n"+
                                  "* (c) 2012, Next Sprint LLC. - Permissive free software license (Apache v2.0)"); 
                return;
            }

            insts = Convert.ToInt32(args[0]);
            tottime = Convert.ToInt32(args[1]);
            secs = Convert.ToInt32(args[2]);
            finishtime = DateTime.Now.AddMinutes(tottime);

            connstr = ConfigurationSettings.AppSettings["ConnectionString"];
            execstr = ConfigurationSettings.AppSettings["ExecuteDbString"];
            writelog = Convert.ToBoolean(ConfigurationSettings.AppSettings["LogOutput"]);
            logfile = ConfigurationSettings.AppSettings["LogFilePath"];
            if (logfile.Trim() == "") logfile = @".\suptass_{DATE}.log";
            if (logfile.Contains("{DATE}")) logfile=logfile.Replace("{DATE}", DateTime.Now.ToString("yyyyMMdd_HHmm"));

            //No need to open this asynchronously. On Windows, all I/O operations smaller than 64 KB will complete synchronously for better performance
            fs = new FileStream(logfile, FileMode.Create, FileAccess.Write, FileShare.Write, 8192, FileOptions.WriteThrough);

            OutputLogMsg(fs, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": Stress test expected to finish by -> " + finishtime, writelog, true);

            threadcount = insts; //when set to 0, all threads are finished.

            for (int i = 0; i < insts; i++)
            {
                Thread t = new Thread(new ParameterizedThreadStart(QueryDb));
                t.Start(i);
            }
            Thread.Sleep(500); // wait for last thread to write to log before declaring all ready
            OutputLogMsg(fs, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": Started all threads", writelog, true);

            while (Interlocked.Read(ref threadcount)>0) { //keep main app alive until the threads are gone
                Thread.Sleep(5000); // check again in 5 secs
            }
            OutputLogMsg(fs, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": Finished all threads, exiting app.", writelog, true);
            OutputLogMsg(fs, "===========================================================\n" +
                             "                    S T A T I S T I C S                    \n" +
                             "-----------------------------------------------------------\n" +
                             "Total number of database queries executed: " + numruns + "\n" +
                             "Maximum response time observed:   " + max_resp_time + " ms\n" +
                             "Minimum response time observed:   " + min_resp_time + " ms\n" +
                             "Average response time observed:   " + tot_resp_time/numruns + " ms\n" +
                             "(Used " + insts + " client connections during " + tot_resp_time/1000 + " secs.)", writelog, true);

            if (writelog) fs.Close();
        }

        static void QueryDb(object id)
        {
            OutputLogMsg(fs, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": Starting thread #" + id.ToString(), writelog, true);

            MyQuery q = new MyQuery(fs);
            q.WriteLog = writelog;
            q.WriteLogFilePath = logfile;
            q.ConnectString = connstr;
            q.SqlString = execstr;
            q.Interval = secs * 1000; //secs to millisecs
            q.EndTime = finishtime;
            q.Execute(Convert.ToInt32(id));

            while (!q.Completed)
            {
                Thread.Sleep(5);
            }

            lock (objLock)
            {
                threadcount = threadcount - 1;
                numruns = numruns + q.ExecutionCount;
                max_resp_time = (q.MaxExecutionTime > max_resp_time ? q.MaxExecutionTime : max_resp_time); //max time for app
                min_resp_time = (q.MinExecutionTime < min_resp_time ? q.MinExecutionTime : min_resp_time); //min time for app
                if (min_resp_time == 0) min_resp_time = q.MinExecutionTime;
                tot_resp_time = tot_resp_time + q.TotalExecutionTime; //total run time for app
            }

            OutputLogMsg(fs, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": Finished calling thread #" + id.ToString(), writelog, true);
        }

        static void OutputLogMsg(FileStream fo, string msg, bool persisted, bool consout)
        {
            if (consout) Console.WriteLine(msg);
            if (persisted)
            {
                msg = msg + "\n";
                byte[] txt = new UTF8Encoding(true).GetBytes(msg);
                lock (objLogLock)
                {
                    fo.Write(txt, 0, txt.Length);
                }
            }
        }
    }
}
