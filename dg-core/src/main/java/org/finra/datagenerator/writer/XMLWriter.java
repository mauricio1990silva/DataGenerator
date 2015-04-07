///*
// * Copyright 2014 DataGenerator Contributors
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.finra.datagenerator.writer;
//
//import org.finra.datagenerator.consumer.DataPipe;
//
//import java.io.OutputStream;
//import java.util.ArrayList;
//
//
///**
// * Created by Mauricio Silva on 4/6/2015.
// */
//public class XMLWriter implements DataWriter {
//
//    private final OutputStream os;
//    private String[] outTemplate;
//    private static ArrayList<String> xmlRecords;
//    private boolean streaming;
//
//
//
//    /**
//     * Constructor
//     *
//     * @param os the output stream to use in writing
//     * @param outTemplate the output template to format writing
//     * @param streaming whether or not output should be streaming on the row level
//     */
//    public XMLWriter(final OutputStream os, final String[] outTemplate, final boolean streaming) {
//
//        this.os = os;
//        this.outTemplate = outTemplate;
//        this.streaming = streaming;
//        xmlRecords = new ArrayList<>();
//
//    }
//
//    @Override
//    public void writeOutput(DataPipe cr) {
//
//
//    }
//
//    @Override
//    public void finish() {
//
//    }
//}
