/*
 * Copyright 2014 DataGenerator Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.finra.datagenerator.writer;

import org.apache.log4j.Logger;
import org.finra.datagenerator.consumer.DataPipe;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;


/**
 * Created by Mauricio Silva on 3/27/2015.
 */
public class SQLWriter implements DataWriter {
    /**
     * Logger
     */
    protected static final Logger log = Logger.getLogger(DefaultWriter.class);
    private final OutputStream os;
    private String[] outTemplate;
    private static ArrayList<String> queries;
    private boolean streaming;
    private String schema;
    private String tableName;
    private String queryFunction;

    /**
     * Constructor
     *
     * @param os the output stream to use in writing
     * @param outTemplate the output template to format writing
     * @param streaming whether or not output should be streaming on the row level
     * @param schema indicates schema of db
     * @param tableName indicates table name of db
     * @param queryFunction insert/update
     */
    public SQLWriter(final OutputStream os, final String[] outTemplate, final boolean streaming,
                     final String schema, final String tableName, final String queryFunction) {

        queries = new ArrayList<>();
        this.os = os;
        this.outTemplate = outTemplate;
        this.streaming = streaming;
        this.schema = schema;
        this.tableName = tableName;
        this.queryFunction = queryFunction;
    }

    @Override
    public void writeOutput(DataPipe cr) {
        String recordInSQL = cr.getQuery(outTemplate, schema, tableName, queryFunction);
        if (null != recordInSQL) {
            if (streaming) {
                try {
                    os.write(recordInSQL.getBytes());
                    os.write("\n".getBytes());
                } catch (IOException e) {
                    log.error("IOException in DefaultConsumer", e);
                }
            } else {
                queries.add(recordInSQL);
            }
        }
    }

    @Override
    public void finish() {
        try {
            for (String query : queries) {
                os.write(query.getBytes());
                os.write("\n".getBytes());
            }
        } catch (IOException e) {
            log.error("IOException in DefaultConsumer", e);
        }
    }
}
