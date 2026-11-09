package today.bonfire.oss.bth4j;

import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * The rule for U+0000 in text the library hands to a store: PostgreSQL cannot keep it, so no store is given it.
 * Text a producer or an operator supplies is refused; text that processor code produces is recorded with
 * U+FFFD in its place, so that a failure is never lost to it.
 */
final class StoredText {
    static final char NUL = '\u0000';
    static final char REPLACEMENT = '\uFFFD';

    private StoredText() {}

    /** @return true when the text is null or has no U+0000 */
    static boolean storable(String text) {
        return text == null || text.indexOf(NUL) < 0;
    }

    /** @throws TaskValidationException when the text has a U+0000 */
    static void require(String field, String text) {
        if (!storable(text)) throw new TaskValidationException(field + " contains U+0000");
    }

    /** @return the text with every U+0000 replaced by U+FFFD; null stays null */
    static String clean(String text) {
        return storable(text) ? text : StringUtils.replaceChars(text, NUL, REPLACEMENT);
    }
}
