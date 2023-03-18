using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FoxDB
{
    public class FoxDB
    {

        bool _IsDbOpen;
        public FileStream _fs;
        readonly string _DbPath;
        readonly string _DbFileName;
        readonly string _DbDirectory;

        public Int32 SumRecords;
        public Int16 StartRecord;
        public Int16 SumTypes;
        public Int16 FieldCount;
        public DbFields[] myFields;

        public struct DbFields
        {
            public string Name;
            public byte Type;
            public byte Width;
            public byte Dec;
            public Int32 wSum;
            public byte[] Value;
        }

        public FoxDB(string DbPath)
        {
            _DbPath = DbPath;
            _DbDirectory = Path.GetDirectoryName(DbPath);
            _DbFileName = Path.GetFileNameWithoutExtension(DbPath);
        }
        public bool OpenDb()
        {
            _IsDbOpen = false;
            try
            {
                if (File.Exists(_DbPath))
                    _fs = File.Open(_DbPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                byte[] h = new byte[32];
                if (_fs.Read(h, 0, h.Length) > 0)
                {
                    if (h[0] != 3 && h[0] != 245)
                    {
                        _fs.Close();
                        return false;
                    }
                    SumRecords = BitConverter.ToInt32(h, 4);
                    StartRecord = BitConverter.ToInt16(h, 8);
                    SumTypes = BitConverter.ToInt16(h, 10);
                    FieldCount = Convert.ToInt16(((StartRecord - 32) / 32));
                }

                myFields = new DbFields[FieldCount];

                for (int r = 0; r < FieldCount; r++)
                {
                    if (_fs.Read(h, 0, h.Length) > 0)
                    {
                        myFields[r].Name = System.Text.Encoding.UTF8.GetString(h, 0, 11).Replace("\0", "").ToUpper();
                        myFields[r].Type = h[11];
                        myFields[r].Dec = h[12];
                        myFields[r].Width = h[16];
                        if (r > 0)
                            myFields[r].wSum = myFields[r - 1].wSum + Convert.ToInt32(h[16]);
                        else
                            myFields[r].wSum = Convert.ToInt32(h[16]);
                        myFields[r].Value = new byte[myFields[r].Width];
                    }
                    else break;
                }

                _IsDbOpen = true;
            }
            catch { }
            return _IsDbOpen;
        }
        public bool CloseDb()
        {
            try
            {
                if (_IsDbOpen) _fs.Close();
                _IsDbOpen = false;
            }
            catch { }
            return _IsDbOpen;
        }
        public void SetSumRecords(Int32 NewSumRecords)
        {
            try
            {
                _fs.Seek(4, SeekOrigin.Begin);
                byte[] xbytes = System.BitConverter.GetBytes(NewSumRecords);
                _fs.Write(xbytes, 0, xbytes.Length);
                SumRecords = NewSumRecords;
            }
            catch { }
        }
        public byte[] ReadByteRecord(Int32 rec)
        {
            if (rec > SumRecords) rec = SumRecords;
            _fs.Seek(1 + StartRecord + rec * SumTypes, SeekOrigin.Begin);
            byte[] h = new byte[SumTypes];
            _fs.Read(h, 0, h.Length);
            return h;
        }
        public string ReadStringRecord(Int32 rec)
        {
            if (rec > SumRecords) rec = SumRecords;
            _fs.Seek(1 + StartRecord + rec * SumTypes, SeekOrigin.Begin);
            byte[] h = new byte[SumTypes];
            _fs.Read(h, 0, h.Length);
            return StringFromBytes(h);
        }
        public string ReadField(Int32 rec, string fldName = "", int fldNumber = -1, bool AsBytes = false)
        {
            string myRead = ReadStringRecord(rec);
            int fld = fldNumber;
            if (fldName != string.Empty)
            {
                fldName = fldName.ToUpper();
                for (int i = 0; i < FieldCount; i++)
                    if (myFields[i].Name == fldName) { fld = i; break; }
            }
            if (fld > FieldCount || fld < 0) fld = FieldCount;

            if (AsBytes)
                return myRead.Substring(myFields[fld].wSum, myFields[fld].Width); //BitConverter.ToString(h); //SinaToWin(h);
            else
            {
                byte[] h = StringToBytes(myRead.Substring(myFields[fld].wSum, myFields[fld].Width));
                return System.Text.Encoding.ASCII.GetString(h).Trim();
            }
        }
        public bool ReadFields(Int32 rec)
        {
            try
            {
                if (rec > SumRecords) rec = SumRecords;
                _fs.Seek(1 + StartRecord + rec * SumTypes, SeekOrigin.Begin);
                for (int i = 0; i < FieldCount; i++)
                    _fs.Read(myFields[i].Value, 0, myFields[i].Value.Length);
                return true;
            }
            catch { }
            return false;
        }
        public bool WriteRecordDb(object[] _Buf, string fldToByte)
        {
            Int32 xSeek = SumRecords * SumTypes + StartRecord;
            if (SumRecords == 0)
            {
                _fs.Seek(xSeek - 2, SeekOrigin.Begin);
                _fs.WriteByte(0);
                _fs.WriteByte(13);
            }
            else _fs.Seek(xSeek, SeekOrigin.Begin);

            _fs.WriteByte(32); //  for * marked
            for (int i = 0; i < FieldCount; i++)
            {
                byte[] h;
                if (myFields[i].Name != fldToByte)
                {
                    //if ((char)myFields[i].Type == 'N')
                    //buf = _Buf[i].ToString().PadLeft(myFields[i].Width);
                    //else
                    //    buf = _Buf[i].PadRight(myFields[i].Width);
                    h = System.Text.Encoding.ASCII.GetBytes(_Buf[i].ToString().PadLeft(myFields[i].Width));
                }
                else h = StringToBytes(_Buf[i].ToString());
                if (h != null) _fs.Write(h, 0, h.Length);
            }
            _fs.WriteByte(26);
            SetSumRecords(SumRecords + 1);

            return true;

        }
        public bool ZapDb(Boolean Zap = false, string Qry = "")
        {
            try
            {
                if (BckDb() == true)
                {
                    string tmpFile = _DbDirectory + _DbFileName + ".tmp";

                    if (OpenDb())
                    {
                        byte[] _buf = new byte[StartRecord];
                        _fs.Seek(0, SeekOrigin.Begin);
                        _fs.Read(_buf, 0, _buf.Length);
                        CloseDb();

                        if (File.Exists(tmpFile)) File.Delete(tmpFile);
                        FileStream _fstmp = File.Open(tmpFile, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                        //SetSumRecords(0);
                        _buf[4] = 0; _buf[5] = 0;
                        _buf[6] = 0; _buf[7] = 0;
                        _buf[StartRecord - 2] = 0x1A;
                        _fstmp.Write(_buf, 0, _buf.Length);
                        _fstmp.Close();
                    }
                    File.Delete(_DbPath);
                    File.Move(tmpFile, _DbPath);

                    return true;
                }
            }
            catch
            {
            }
            CloseDb();
            return false;
        }
        public bool BckDb()
        {
            try
            {
                if (File.Exists(_DbDirectory + _DbFileName + ".bck"))
                    File.Delete(_DbDirectory + _DbFileName + ".bck");
                File.Copy(_DbPath, _DbDirectory + _DbFileName + ".bck");
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static byte[] StringToBytes(string str)
        {
            byte[] array = new byte[str.Length * 2];
            for (int i = 0; i < str.Length; i++)
            {
                char c = str[i];
                array[i * 2] = (byte)(c & 0xFF);
                array[i * 2 + 1] = (byte)((c & 0xFF00) >> 8);
            }

            return array;
        }
        public static string StringFromBytes(byte[] arr)
        {
            char[] array = new char[arr.Length / 2];
            for (int i = 0; i < array.Length; i++)
            {
                array[i] = (char)(arr[i * 2] + (arr[i * 2 + 1] << 8));
            }

            return new string(array);
        }

    }
}
