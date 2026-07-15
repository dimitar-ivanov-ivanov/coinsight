package coinsight.arbitrage.shared.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ExceptionUtils {

    /**
     * Walks to the deepest cause and returns its message. Spring's exception translation
     * (e.g. BadSqlGrammarException) nests the actual driver/DB error as a cause rather than
     * including it in the top-level getMessage() - logging just e.getMessage() on one of
     * these silently drops the one piece of information that actually explains what went
     * wrong (e.g. "relation does not exist" vs a genuine syntax error).
     *
     * @param throwable the exception to unwrap
     * @return the deepest cause's "ClassName: message", or the original message if there's no cause
     */
    public static String rootCauseMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getClass().getSimpleName() + ": " + current.getMessage();
    }
}
