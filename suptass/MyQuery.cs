using System;
using System.Text;
using System.Data.SqlClient;
using System.Threading;
using System.IO;

namespace suptass
{
    class MyQuery
    {
        private int m_tid; //thread id
        private Random m_rnd = new Random();
        private FileStream m_fs;
        
        private long m_execcnt = 0;
        private double m_tottime = 0;
        private double m_maxtime = 0;
        private double m_mintime = 0;

        public bool WriteLog { get; set; }
        public string WriteLogFilePath { get; set; }
        public string ConnectString { get; set; }
        public string SqlString { get; set; }
        public DateTime EndTime { get; set; }
        public int Interval { get; set; }
        public bool Completed { get; set; }

        public long ExecutionCount {
            get
            {
                return m_execcnt;
            }
        }
        public double MaxExecutionTime {
            get
            {
                return m_maxtime;
            }
        }
        public double MinExecutionTime {
            get
            {
                return m_mintime;
            }
        }
        public double TotalExecutionTime {
            get
            {
                return m_tottime;
            }
        }

        public MyQuery(FileStream f)
        {
            m_fs = f;
        }

        public void Execute(int tid) {
            m_tid = tid;
            m_rnd = new Random(tid);
            //m_fs = new FileStream(WriteLogFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Write, 8192, FileOptions.WriteThrough);

            Timer t = new Timer(new TimerCallback(OnTimedEvent), null, 0, Interval);
            //t.Interval = Interval;
            //t.Elapsed += new ElapsedEventHandler(OnTimedEvent);
            //t.AutoReset = true;
            //t.Enabled = true;
            this.Completed = false;

            while (!Completed)
            {
                Thread.Sleep(1);
            }
        }

        private void ExecuteCommand()
        {
            //we're modifying the connection string to prevent .Net connection pooling...
            string connstr = ConnectString + "Application Name=suptass.exe (client: " + m_tid + ");";
            long rt = 0;
            int numrecs = -1;
            m_execcnt = m_execcnt + 1;

            DateTime dt_start = DateTime.Now;
            DateTime dt_end = DateTime.Now;

            OutputLogMsg(m_fs, dt_start.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": Starting " + m_execcnt + 
                (m_execcnt==1 ? "st":(m_execcnt==2?"nd":(m_execcnt==3?"rd":"th"))) +
                " run in thread #" + m_tid, WriteLog, false);

            SqlConnection myconn = new SqlConnection(connstr);
            myconn.Open();
            SqlCommand cmd = new SqlCommand(SqlString, myconn);
            SqlDataReader rdr = null;

            try
            {
                rdr = cmd.ExecuteReader();

                if (rdr.HasRows)
                {
                    numrecs = 0;
                    // data access code
                    while (rdr.Read())
                    {
                        //we don't need the info, just iterating through it...
                        numrecs = numrecs + 1;
                    }
                }
                else
                {
                    //do nothing
                }
            }
            catch (SqlException sqe)
            {
                OutputLogMsg(m_fs, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": SQL Exception in thread #" + m_tid +
                    "\n>>>>>>>>ERROR INFO:>>>>" + sqe.ToString(), WriteLog, true);
            }
            finally
            {
                 if (rdr != null)
                 {
                     rdr.Close();
                 }
                 if (myconn != null)
                 {
                     myconn.Close();
                 }
            }
            dt_end = DateTime.Now;
            rt = (dt_end - dt_start).Milliseconds; //run time for this iteration
            m_maxtime = (rt > m_maxtime ? rt : m_maxtime); //max time for thread
            m_mintime = (rt < m_mintime ? rt : m_mintime); //min time for thread
            if (m_mintime == 0) m_mintime = rt; //
            m_tottime = m_tottime + rt; //total run time for life of thread

            OutputLogMsg(m_fs, dt_end.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": " + (numrecs == -1 ? "No records found" : "Iterated through " + numrecs + " records") + " in thread #" + m_tid, WriteLog,false);
            OutputLogMsg(m_fs, dt_end.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": " + "Query completed in " + rt + " milliseconds for cycle " + m_execcnt + ", thread #" + m_tid, WriteLog, false);
        }

        public void OnTimedEvent(object state)
        {
            if (DateTime.Now >= EndTime)
            {
                OutputLogMsg(m_fs, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ": Ending time detected for thread #" + m_tid, WriteLog, false);
                this.Completed = true;
                return;
            }
            //to randomly spread DB access between multiple threads, we'll try to add a small random delay
            int i = m_rnd.Next(Convert.ToInt32(Interval/10)); //limit length of delay
            Thread.Sleep(i);

            ExecuteCommand();
        }

        private void OutputLogMsg(FileStream fo, string msg, bool persisted, bool consout)
        {
            if (consout) Console.WriteLine(msg);
            if (persisted)
            {
                msg = msg + "\n";
                byte[] txt = new UTF8Encoding(true).GetBytes(msg);
                lock (suptass.objLogLock)
                {
                    fo.Write(txt, 0, txt.Length);
                }
            }
        }
    }
}
