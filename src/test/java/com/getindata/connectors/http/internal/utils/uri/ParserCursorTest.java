package com.getindata.connectors.http.internal.utils.uri;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParserCursorTest {

    @Test
    public void testBoundsValidation() {

        assertAll(() -> {
                assertThrows(IndexOutOfBoundsException.class, () -> new ParserCursor(-1, 1));
                assertThrows(IndexOutOfBoundsException.class, () -> new ParserCursor(1, -1));
            }
        );
    }

    @Test
    public void testUpdatePostValidation() {
        ParserCursor cursor = new ParserCursor(1, 2);

        assertAll(() -> {
                assertThrows(IndexOutOfBoundsException.class, () -> cursor.updatePos(0));
                assertThrows(IndexOutOfBoundsException.class, () -> cursor.updatePos(3));
            }
        );
    }
}
