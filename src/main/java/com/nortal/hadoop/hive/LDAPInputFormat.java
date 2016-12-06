package com.nortal.hadoop.hive;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.directory.shared.ldap.ldif.LdapLdifException;
import org.apache.directory.shared.ldap.ldif.LdifEntry;
import org.apache.directory.shared.ldap.ldif.LdifReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Marko Kaar on 01/12/16.
 * Reads LDIF Entities records starting with the first "dn: ..." attribute. Input files can be zipped or simply be ldif files.
 */

public class LDAPInputFormat extends FileInputFormat {

    final static Logger logger = LoggerFactory.getLogger(LDAPInputFormat.class);

    @Override
    public RecordReader<LongWritable, ObjectWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        Path path = fileSplit.getPath();
        long start = 0L;
        long length = fileSplit.getLength();
        logger.info("createRecordReader done");
        try {
            return initLDAPRecordReader(path, start, length, jobConf);
        } catch (LdapLdifException e) {
            throw new IOException(e);
        }
    }

    private LDAPRecordReader initLDAPRecordReader(Path path, long start, long length, JobConf conf) throws IOException, LdapLdifException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream baseStream = fs.open(path);
        DataInputStream stream = baseStream;
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecs.getCodec(path);
        if (codec != null)
            stream = new DataInputStream(codec.createInputStream(stream));
        LdifReader reader = new LdifReader();

        logger.info("initRecordReader done");

        return new LDAPRecordReader(reader, start, length, baseStream, stream);

    }

    public static class LDAPRecordReader implements RecordReader<LongWritable, ObjectWritable> {

        final static Logger logger = LoggerFactory.getLogger(LDAPInputFormat.class);

        LdifReader ldifReader;
        Iterator<LdifEntry> ldifReaderIterator;
        Seekable baseStream;
        DataInputStream stream;
        long entryCount = 0;
        long start, end;
        private LongWritable key = new LongWritable();
        private ObjectWritable value = new ObjectWritable();

        public LDAPRecordReader(LdifReader reader, long start, long length, FSDataInputStream baseStream, DataInputStream stream) throws IOException, LdapLdifException {
            this.ldifReader = reader;
            this.baseStream = baseStream;
            this.stream = stream;
            this.start = start;
            this.end = length;

            String ldif = new String(trim(IOUtils.toByteArray(stream)), "UTF-8").replace("::", ":");

            logger.info("StreamBytes: " + Arrays.toString(ldif.getBytes()));
            logger.info("TextStringBytes: " + ldif);
            List<LdifEntry> ldifEntries = reader.parseLdif(ldif);

            ldifReaderIterator = ldifEntries.iterator();
        }

        private static byte[] trim(byte[] bytes)
        {
            int i = bytes.length - 1;
            while (i >= 0 && bytes[i] == 0)
            {
                --i;

            }
            return Arrays.copyOf(bytes, i + 1);
        }

        @Override
        public boolean next(LongWritable longWritable, ObjectWritable text) throws IOException {
            if(!ldifReaderIterator.hasNext())
                return false;

            key.set(++entryCount);
            value.set(ldifReaderIterator.next());

            return true;
        }

        @Override
        public LongWritable createKey() {
            return key;
        }

        @Override
        public ObjectWritable createValue() {
            return value;
        }

        public long getPos() throws IOException {
            return baseStream.getPos();
        }

        @Override
        public float getProgress() throws IOException {
            if (start == end)
                return 0;
            return Math.min(1.0f, (getPos() - start) / (float)(end - start));
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }


    }

}

