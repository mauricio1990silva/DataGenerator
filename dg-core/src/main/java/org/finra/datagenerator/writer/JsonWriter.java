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
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;


/**
 * Created by Mauricio Silva on 3/26/2015.
 */
public class JsonWriter implements DataWriter {


    /**
     * Logger
     */
    protected static final Logger log = Logger.getLogger(DefaultWriter.class);
    private final OutputStream os;
    private String[] outTemplate;
    private static JSONArray jsonArray;
    private boolean streaming;

    /**
     * Constructor
     *
     * @param os the output stream to use in writing
     * @param outTemplate the output template to format writing
     * @param streaming whether or not output should be streaming as it moves on
     */
    public JsonWriter(final OutputStream os, final String[] outTemplate, final boolean streaming) {

        this.os = os;
        this.outTemplate = outTemplate;
        this.streaming = streaming;
        jsonArray = new JSONArray();
    }
    @Override
    public void writeOutput(DataPipe cr) {

        JSONObject recordInJson = cr.getJsonFormat(outTemplate);
        if (null != recordInJson) {
            if (streaming) {
                try {
                    os.write(recordInJson.toString(3).getBytes());
                    os.write("\n".getBytes());
                } catch (IOException e) {
                    log.error("IOException in DefaultConsumer", e);
                }
            } else {
                jsonArray.put(recordInJson);
            }
        }
    }

    @Override
    public void finish() {
        try {
            os.write(jsonArray.toString(3).getBytes());
            os.write("\n".getBytes());
        } catch (IOException e) {
            log.error("IOException in DefaultConsumer", e);
        }
    }

}
