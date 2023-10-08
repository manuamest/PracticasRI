import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.*;

public class TopTermsInDocs {
    public static void main(String[] args) throws Exception {
        String indexPath = null;
        String docID = null;
        String outFilePath = null;
        int top = 0;

        // Interpretar los argumentos de la línea de comandos
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docID":
                    docID = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outFilePath = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        // Verificar si se proporcionó una ruta de índice válida
        if (indexPath == null) {
            System.out.println("Please provide a valid path");
            System.exit(1);
        }

        Directory dir = null;
        DirectoryReader indexReader = null;
        List<Integer> documentsToCheck = new ArrayList<>();
        // Procesar el rango de ID de documento si se proporciona
        if (docID != null) {
            int rangoI = Integer.parseInt(docID.split("-")[0]);
            int rangoF = Integer.parseInt(docID.split("-")[1]);

            for (int i = rangoI; i != (rangoF + 1); i++) {
                documentsToCheck.add(i);
            }
        }

        // Abrir el índice y el lector
        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
            System.exit(1);
        }

        // Obtener información sobre el campo
        final FieldInfos fieldinfos = FieldInfos.getMergedFieldInfos(indexReader);
        int docCount = indexReader.numDocs();
        System.out.println("NUM DOCS: " + docCount);

        // Si se proporciona una ruta de salida, crear un PrintWriter
        PrintWriter outFile = null;
        if (outFilePath != null) {
            outFile = new PrintWriter(outFilePath);
        }

        // Imprimir encabezado
        String header = String.format("%-20s%-10s%-25s%-20s%-10s%-20s%-80s\n", "TERM", "DOCID", "FIELD", "FREQ", "DOC_FREQ","raw tf x idflog10", "POSITIONS (-1 means No Positions indexed for this field)");
        System.out.print(header);
        if (outFile != null) {
            outFile.print(header);
        }

        // Procesar términos en el campo "contents"
        if (fieldinfos.fieldInfo("contents") != null) {
            final Terms terms = MultiTerms.getTerms(indexReader, "contents");

            if (terms != null) {
                // Calcular la relevancia de cada término
                HashMap<String, Float> mapeo = new HashMap<>();
                TFIDFSimilarity tfidf = new ClassicSimilarity();
                final TermsEnum termsEnum = terms.iterator();

                while (termsEnum.next() != null) {
                    String termString = termsEnum.term().utf8ToString();

                    Term term = new Term("contents", termString);

                    int docFreq = indexReader.docFreq(term);

                    float idf = tfidf.idf(docFreq, docCount);

                    float relevance = docFreq * idf;

                    mapeo.putIfAbsent(termString, relevance);

                }
                // Ordenar términos por relevancia
                List<Map.Entry<String, Float>> list = new ArrayList<>(mapeo.entrySet());
                Comparator<Map.Entry<String, Float>> comparator = (o1, o2) -> o2.getValue().compareTo(o1.getValue());
                list.sort(comparator);
                LinkedHashMap<String, Float> sortedMap = new LinkedHashMap<>();
                for (Map.Entry<String, Float> entry : list) {
                    sortedMap.put(entry.getKey(), entry.getValue());
                }

                int currentTerm = 0;
                for (String key : sortedMap.keySet()) {
                    if (currentTerm == top && top >= 0) break;
                    PostingsEnum posting = MultiTerms.getTermPostingsEnum(indexReader, "contents", new BytesRef(key));
                    Term term = new Term("contents", key);
                    int docFreq = indexReader.docFreq(term);

                    if (posting != null) {
                        int currentDocId;

                        while ((currentDocId = posting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
                            if (documentsToCheck.contains(currentDocId)) {
                                int freq = posting.freq();
                                String output = String.format("%-20s%-10d%-25s%-20s%-10d%-20s", key, currentDocId, "contents", freq,docFreq, sortedMap.get(key));
                                System.out.print(output);
                                if (outFile != null) {
                                    outFile.print(output);
                                }
                                for (int i = 0; i < freq; i++) {
                                    // Agarra la posición de la siguiente ocurrencia del término en el documento actual.
                                    String position = (i > 0 ? "," : "") + posting.nextPosition();
                                    System.out.print(position);
                                    if (outFile != null) {
                                        outFile.print(position);
                                    }
                                }
                                System.out.println();
                                if (outFile != null) {
                                    outFile.println();
                                }
                            }
                        }
                    }
                    currentTerm++;
                }
            }
        }

        // Cerrar el lector, el directorio y el archivo de salida (si corresponde)
        try {
            indexReader.close();
            dir.close();
            if (outFile != null) {
                outFile.close();
            }
        } catch (IOException e) {
            System.out.println("Graceful message: exception " + e);
            e.printStackTrace();
        }
    }
}

