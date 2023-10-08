import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.nio.file.Files;

public class RemoveDuplicates {
    public static void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static void main(String[] args) throws IOException {
        // Variables para el index y el output
        String indexPath = "./index";
        String outFilePath = "./output";

        // Interprete de comandos
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-out":
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

        Directory indexDirectory;
        IndexReader indexReader = null;

        // Abre el indice inicial y el lector
        try {
            indexDirectory = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(indexDirectory);
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
            System.exit(1);
        }

        // Printea los argumentos principales
        System.out.println("Original index: " + indexPath);
        System.out.println("Number of documents: " + indexReader.numDocs());
        System.out.println("Number of terms: " + indexReader.getSumTotalTermFreq("contents"));

        // Crea un nuevo directorio para el indice sin duplicados
        deleteDirectory(Paths.get(outFilePath));
        Directory outDirectory = FSDirectory.open(Paths.get(outFilePath));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter outIndexWriter = new IndexWriter(outDirectory, config);

        // Usan un HashSet para almacenar los documentos unicos
        Set<BytesRef> uniqueDocs = new HashSet<>();
        for (int i = 0; i < indexReader.maxDoc(); i++) {
            Document doc = indexReader.document(i);
            BytesRef contents;
            // Recupera los contenidos
            String content = doc.get("contents");
            if (content == null) {
                content = "";
            }
            contents = new BytesRef(content);
            // Si los contenidos son unicos los añade al nuevo indice
            if (uniqueDocs.add(contents)) {
                outIndexWriter.addDocument(doc);
            }
        }

        // Actualizamos y cerramos
        outIndexWriter.commit();
        outIndexWriter.close();
        // Abrimos el nuevo indice para ver los resultados
        IndexReader outIndexReader = DirectoryReader.open(outDirectory);
        System.out.println("New index without duplicates: " + outFilePath);
        System.out.println("Number of documents: " + outIndexReader.numDocs());
        System.out.println("Number of terms: " + outIndexReader.getSumTotalTermFreq("contents"));

        // Cierra los indexReaders
        indexReader.close();
        outIndexReader.close();
    }
}
