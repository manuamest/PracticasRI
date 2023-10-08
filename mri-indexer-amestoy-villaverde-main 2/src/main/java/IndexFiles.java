/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class IndexFiles {
    private static final String CONFIG_PROPERTIES_PATH = "./src/main/resources/config.properties";
    static final String KNN_DICT = "knn-dict";

    // Calculates embedding vectors for KnnVector search
    private final DemoEmbeddings demoEmbeddings;
    private final KnnVectorDict vectorDict;

    public static class WorkerThread implements Runnable {
        private final Path folder;
        private final IndexFiles indexFiles;
        private final Directory partialIndexDir;
        private final boolean contentsStored;
        private final boolean contentsTermVectors;
        private final String[] onlyFilesArray;
        private final String[] notFilesArray;
        private final String firstEntry;
        private final int onlyLines;
        int depth;// default value

        public WorkerThread(final Path folder,IndexFiles indexFiles, Directory partialIndexDir,boolean contentsStored,
                            boolean contentsTermVectors, String[] onlyFilesArray, String[] notFilesArray,
                            String firstEntry, int onlyLines, int depth)
        {
            this.folder = folder;
            this.indexFiles = indexFiles;
            this.partialIndexDir = partialIndexDir;
            this.contentsStored = contentsStored;
            this.contentsTermVectors = contentsTermVectors;
            this.onlyFilesArray = onlyFilesArray;
            this.notFilesArray = notFilesArray;
            this.firstEntry = firstEntry;
            this.onlyLines = onlyLines;
            this.depth = depth;
        }
        @Override
        public void run() {
            System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'", Thread.currentThread().getName(), folder));
            try {
                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig partialIwc = new IndexWriterConfig(analyzer);

                IndexWriter writer = new IndexWriter(partialIndexDir, partialIwc);


                indexFiles.indexDocs(writer, folder, contentsStored, contentsTermVectors, onlyFilesArray, notFilesArray, firstEntry, onlyLines, depth);

                writer.close();

            } catch (IOException e) {
                System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
            }
            System.out.println(String.format("I am the thread '%s' and I finished indexing the folder '%s'", Thread.currentThread().getName(), folder));

        }

    }
    private IndexFiles(KnnVectorDict vectorDict) throws IOException {
        if (vectorDict != null) {
            this.vectorDict = vectorDict;
            demoEmbeddings = new DemoEmbeddings(vectorDict);
        } else {
            this.vectorDict = null;
            demoEmbeddings = null;
        }
    }
    private static String readPropertiesFile(BufferedReader reader2, InputStream stream) throws IOException {
        String firstEntry = null;
        StringBuilder sb = new StringBuilder();
        String line;
        boolean firstEntryAlreadyGot = false;
        int i = 0;

        while ((line = reader2.readLine()) != null) {
            if (line.split("=")[0].trim().compareTo("onlyFiles") == 0 || line.split("=")[0].trim().compareTo("notFiles") == 0 ) {
                if (!firstEntryAlreadyGot) {
                    firstEntry = line.split("=")[0].trim();
                    firstEntryAlreadyGot = true;
                }
            }
            sb.append(line);
            sb.append(System.lineSeparator());
            i++;
        }

        reader2.close();
        stream.close();

        return firstEntry;
    }

    public static void main(String[] args) throws Exception {
        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-knn_dict DICT_PATH] [-numThreads N_THREADS] [-depth N_DEPTH] [-contentsStored] [-contentsTermVectors]\n\n"
                        + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                        + "in INDEX_PATH that can be searched with SearchFiles\n"
                        + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
        String indexPath = "./index";
        String docsPath = null;
        String vectorDictSource = null;
        boolean create = true;
        int numThreads = Runtime.getRuntime().availableProcessors();
        int depth = Integer.MAX_VALUE;
        boolean contentsStored = false;
        boolean contentsTermVectors = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    //Path del indice
                    indexPath = args[++i];
                    break;
                case "-docs":
                    //Path a indexar
                    docsPath = args[++i];
                    break;
                case "-knn_dict":
                    vectorDictSource = args[++i];
                    break;
                case "-update":
                    //Actualizar el indice ya existente
                    create = false;
                    break;
                case "-create":
                    //Crear nuevo indice
                    create = true;
                    break;
                case "-numThreads":
                    //Numero de threads
                    numThreads = Integer.parseInt(args[++i]);
                    break;
                case "-depth":
                    //Profundidad de indexacion
                    depth = Integer.parseInt(args[++i]);
                    break;
                case "-contentsStored":
                    //Campo contents debe ser stored
                    contentsStored = true;
                    break;
                case "-contentsTermVectors":
                    //Infica que se debe almacenar Term vectors en contents
                    contentsTermVectors = true;
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println(
                    "Document directory '"
                            + docDir.toAbsolutePath()
                            + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        System.out.println("Indexing to directory '" + indexPath + "'...");

        Directory dir = FSDirectory.open(Paths.get(indexPath)); //Directorio del index
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer); //Configuracion del writer

        if (create) {
            // Create a new index in the directory, removing any previously indexed documents:
            iwc.setOpenMode(OpenMode.CREATE);
        } else {
            // Add new documents to an existing index:
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        }

        KnnVectorDict vectorDictInstance = null;
        long vectorDictSize = 0;
        if (vectorDictSource != null) {
            KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
            vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
            vectorDictSize = vectorDictInstance.ramBytesUsed();
        }
        //Lectura del fichero config.properties
        Properties properties = new Properties();
        final Path CONFIG_PATH = Paths.get(CONFIG_PROPERTIES_PATH);
        InputStream stream = Files.newInputStream(CONFIG_PATH);
        BufferedReader reader2 = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        String firstEntry = readPropertiesFile(reader2, stream);

        stream = Files.newInputStream(CONFIG_PATH);
        reader2 = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));

        properties.load(reader2);

        String[] onlyFilesArray = null;
        String[] notFilesArray = null;
        int onlyLines = -1;
        if (properties.getProperty("onlyFiles") != null) {
            onlyFilesArray = properties.getProperty("onlyFiles").split(" ");
        }
        if (properties.getProperty("notFiles") != null) {
            notFilesArray = properties.getProperty("notFiles").split(" ");
        }
        if (properties.getProperty("onlyLines") != null) {
            onlyLines = Integer.parseInt(properties.getProperty("onlyLines"));
        }

        reader2.close();
        stream.close();

        //Inicializamos los hilos
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        //Abrimos el IndexWriter
        IndexWriter writer = new IndexWriter(dir, iwc);
        IndexFiles indexFiles = new IndexFiles(vectorDictInstance);

        //Array al que se iran añadiendo los directorios de indices parciales, para despues juntarlos todos en el final
        ArrayList<Directory> partialIndexDirArray = new ArrayList<>();
        int indexDirNumber = 1;

        try  {
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir)) {
                /* Procesamos cada subcarpeta en un nuevo hilo. Los ficheros de primer nivel no se indexan */
                for (final Path path : directoryStream) {
                    if (Files.isDirectory(path)) {
                        Directory partialIndexDir = FSDirectory.open(Paths.get(indexPath + indexDirNumber));
                        partialIndexDirArray.add(partialIndexDir); //Añadimos el directorio del indice al array
                        indexDirNumber++;

                        //Instanciamos el workerThread y lanzamos los hilos
                        final Runnable worker = new WorkerThread(path, indexFiles,partialIndexDir,  contentsStored, contentsTermVectors, onlyFilesArray, notFilesArray, firstEntry, onlyLines, depth);
                        executor.execute(worker);
                    }
                }
                //Close the ThreadPool; no more jobs will be accepted, but all the previously submitted jobs will be processed.
                executor.shutdown();
                //Wait up to 1 hour to finish all the previously submitted jobs
                try {
                    executor.awaitTermination(1, TimeUnit.HOURS);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                    System.exit(-2);
                }

                System.out.println("Finished all threads");

                //Juntamos todos los indices parciales en uno solo (index)
                for (Directory directory : partialIndexDirArray) {
                    writer.addIndexes(directory);
                }

            } catch (IOException e) {
                System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
            }

        } finally {
            //Hacemos commit del writer y cerramos
            writer.commit();
            writer.close();
            IOUtils.close(vectorDictInstance);
        }



        Date end = new Date();
        try (IndexReader reader = DirectoryReader.open(dir)) {
            System.out.println(
                    "Indexed "
                            + reader.numDocs()
                            + " documents in "
                            + (end.getTime() - start.getTime())
                            + " milliseconds");
            if (reader.numDocs() > 100
                    && vectorDictSize < 1_000_000
                    && System.getProperty("smoketester") == null) {
                throw new RuntimeException(
                        "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
            }
        }
    }



    void indexOrNotDoc(IndexWriter writer, Path file, long lastModified, boolean contentsStored, boolean contentsTermVectors, String firstEntry, String[] onlyFilesArray, String[] notFilesArray, int onlyLines) throws IOException {
        if (firstEntry != null) { //Si existe onlyFiles o notFiles...
            if (firstEntry.compareTo("onlyFiles") == 0) { //Si es onlyFiles aparece primero, hazle caso a onlyFiles
                for (String entry : onlyFilesArray) {
                    if (file.toString().endsWith(entry)) {
                        indexDoc(writer, file, lastModified, contentsStored, contentsTermVectors, onlyLines);
                    }
                }
            } else { //Si notFiles aparece primero, hazle caso a notFiles
                for (String entry : notFilesArray) {
                    if (!file.toString().endsWith(entry)) {
                        indexDoc(writer, file, lastModified, contentsStored, contentsTermVectors, onlyLines);
                    }
                }
            }

        }
        else { //Si no existe onlyFiles ni notFiles, indexamos
            indexDoc(writer, file, lastModified, contentsStored, contentsTermVectors, onlyLines);
        }
    }
    /**
     * Indexes the given file using the given writer, or if a directory is given, recurses over files
     * and directories found under the given directory.
     *
     * <p>NOTE: This method indexes one document per input file. This is slow. For good throughput,
     * put multiple documents into your input file(s). An example of this is in the benchmark module,
     * which can create "line doc" files, one document per line, using the <a
     * href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be stored
     * @param path The file to index, or the directory to recurse into to find files to index
     * @throws IOException If there is a low-level I/O error
     */
    void indexDocs(final IndexWriter writer, Path path, boolean contentsStored, boolean contentsTermVectors,String[] onlyFilesArray, String[] notFilesArray, String firstEntry, int onlyLines, int depth) throws IOException {

        if (Files.isDirectory(path)) {
            Files.walkFileTree(
                    path,
                    EnumSet.noneOf(FileVisitOption.class),
                    depth,
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            try {
                                indexOrNotDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, firstEntry, onlyFilesArray, notFilesArray, onlyLines);
                            } catch (
                                    @SuppressWarnings("unused")
                                    IOException ignore) {
                                ignore.printStackTrace(System.err);
                                // don't index files that can't be read.
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        } else {
            indexOrNotDoc(writer, path, Files.getLastModifiedTime(path).toMillis(), contentsStored, contentsTermVectors, firstEntry, onlyFilesArray, notFilesArray, onlyLines);
        }
    }

    /** Indexes a single document */
    void indexDoc(IndexWriter writer, Path file, long lastModified, boolean contentsStored, boolean contentsTermVectors, int onlyLines) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            // make a new, empty document
            Document doc = new Document();

            // Add the path of the file as a field named "path".  Use a
            // field that is indexed (i.e. searchable), but don't tokenize
            // the field into separate words and don't index term frequency
            // or positional information:
            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);

            // Add the last modified date of the file a field named "modified".
            // Use a LongPoint that is indexed (i.e. efficiently filterable with
            // PointRangeQuery).  This indexes to milli-second resolution, which
            // is often too fine.  You could instead create a number based on
            // year/month/day/hour/minutes/seconds, down the resolution you require.
            // For example the long value 2011021714 would mean
            // February 17, 2011, 2-3 PM.
            doc.add(new LongPoint("modified", lastModified));

            // Add the contents of the file to a field named "contents".  Specify a Reader,
            // so that the text of the file is tokenized and indexed, but not stored.
            // Note that FileReader expects the file to be in UTF-8 encoding.
            // If that's not the case searching for special characters will fail.

            FieldType t = new FieldType();
            t.setTokenized(true);
            t.setOmitNorms(true);
            t.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

            if (contentsTermVectors) {
                t.setStoreTermVectors(true);
            }

            int linesRead = 0;
            if (contentsStored) { //Añadimos el campo contents como stored
                t.setStored(true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    if (linesRead == onlyLines) break;
                    linesRead++;

                    sb.append(line);
                    sb.append(System.lineSeparator());
                }
                String contents = sb.toString();

                doc.add(new Field("contents", contents,t));
                reader.close();
            }
            else { //Añadimos el campo contents como not-stored
                t.setStored(false);
                doc.add(new Field("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)), t));
            }

            //Añadimos campo hostname, nombre del host que se encargo de la indexación de este documento
            doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
            //Añadimos el campo thread, nombre del thread que se encargo de la indexación de este documento
            doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));

            //Añadimos el campo type, tipo del documento
            if (Files.isRegularFile(file))
                doc.add(new StringField("type", "regular file", Field.Store.YES));
            else if (Files.isDirectory(file))
                doc.add(new StringField("type", "directory", Field.Store.YES));
            else if (Files.isSymbolicLink(file))
                doc.add(new StringField("type", "symbolic link", Field.Store.YES));
            else
                doc.add(new StringField("type", "otro", Field.Store.YES));

            //Añadimos el campo sizeKbString y sizeKbLongPoint
            doc.add(new StringField("sizeKbString", String.valueOf(Files.size(file)), Field.Store.YES));
            doc.add(new LongPoint("sizeKbLongPoint", Files.size(file)));


            //Añadimos las fechas de creacion, ultima modificacion y ultimo acceso al documento.
            FileTime creationTime = (FileTime) Files.getAttribute(file, "creationTime");
            FileTime lastModifiedTime = Files.getLastModifiedTime(file);
            FileTime lastAccessTime = (FileTime) Files.getAttribute(file, "lastAccessTime");

            doc.add(new StringField("creationTime", creationTime.toString(), Field.Store.YES));
            doc.add(new StringField("lastModifiedTime", lastModifiedTime.toString(), Field.Store.YES));
            doc.add(new StringField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES));

            //Añadimos las fechas de creacion, ultima modificacion y ultimo acceso al documento, pero en el formato de lucene
            Date creationTimeDate = new Date(creationTime.toMillis());
            Date lastModifiedTimeDate = new Date(lastModifiedTime.toMillis());
            Date lastAccessTimeDate = new Date(lastAccessTime.toMillis());

            String creationTimeLucene = DateTools.dateToString(creationTimeDate, DateTools.Resolution.MILLISECOND);
            String lastModifiedTimeLucene = DateTools.dateToString(lastModifiedTimeDate, DateTools.Resolution.MILLISECOND);
            String lastAccessTimeLucene = DateTools.dateToString(lastAccessTimeDate, DateTools.Resolution.MILLISECOND);

            doc.add(new StringField("creationTimeLucene", creationTimeLucene, Field.Store.YES));
            doc.add(new StringField("lastModifiedTimeLucene", lastModifiedTimeLucene, Field.Store.YES));
            doc.add(new StringField("lastAccessTimeLucene", lastAccessTimeLucene, Field.Store.YES));



            if (demoEmbeddings != null) {
                try (InputStream in = Files.newInputStream(file)) {
                    float[] vector =
                            demoEmbeddings.computeEmbedding(
                                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
                    doc.add(
                            new KnnVectorField("contents-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
                }
            }

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                // New index, so we just add the document (no old document can be there):
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                // Existing index (an old copy of this document may have been indexed) so
                // we use updateDocument instead to replace the old one matching the exact
                // path, if present:
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }
}

