package htsjdk.samtools.cram.encoding.external;

import htsjdk.samtools.util.RuntimeIOException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Encode byte arrays by specifying a stop byte to separate the arrays.
 * This cannot be a byte that appears in the data.
 */
final class ByteArrayStopCodec extends ExternalCodec<byte[]> {
    private final int stop;
    private final PushbackInputStream pushBackInputStream;
    private static final int BUFFER_SIZE = 152;
    
    /**
     * Construct a Byte Array Stop Codec
     *
     * @param inputStream the input bytestream to read from
     * @param outputStream the output bytestream to write to
     * @param stopByte the byte used to mark array boundaries
     */
    public ByteArrayStopCodec(final ByteArrayInputStream inputStream,
                              final ByteArrayOutputStream outputStream,
                              final byte stopByte) {
        super(inputStream, outputStream);
        this.stop = 0xFF & stopByte;
        pushBackInputStream = new PushbackInputStream(inputStream, BUFFER_SIZE);
    }

    @Override
    public byte[] read() {
        final ByteArrayOutputStream readingBAOS = new ByteArrayOutputStream(BUFFER_SIZE);
        byte [] buffer = new byte[BUFFER_SIZE];
        int numberOfBytesRead;
        try {
            while ((numberOfBytesRead = pushBackInputStream.read(buffer)) != -1) {
                for (int i = 0; i < numberOfBytesRead; i++) {
                    if ((buffer[i] & 0xFF) == stop) {
                        // Write all bytes up to the stop byte
                        readingBAOS.write(buffer, 0, i);
                        // Unread the remaining bytes (i+1 to bytesRead)
                        pushBackInputStream.unread(buffer, i + 1, numberOfBytesRead - (i + 1));
                        return readingBAOS.toByteArray();
                    }
                }
                readingBAOS.write(buffer, 0, numberOfBytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return readingBAOS.toByteArray();
    }

    @Override
    public byte[] read(final int length) {
        throw new RuntimeException("Not implemented.");
    }

    @Override
    public void write(final byte[] value) {
        try {
            outputStream.write(value);
            outputStream.write(stop);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }
}
