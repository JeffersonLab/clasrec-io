package org.jlab.clas.std.services.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public final class FileUtils {

    private FileUtils() { }

    public static void deleteFileTree(Path dir) throws IOException {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException ex)
                            throws IOException {
                        if (ex == null) {
                            Files.deleteIfExists(dir);
                            return FileVisitResult.CONTINUE;
                        } else if (ex instanceof NoSuchFileException) {
                            return FileVisitResult.CONTINUE;
                        }
                        throw ex;
                    }
            });
        } catch (NoSuchFileException e) {
            // ignore
        }
    }
}
