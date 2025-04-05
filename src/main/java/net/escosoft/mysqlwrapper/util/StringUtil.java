package net.escosoft.mysqlwrapper.util;

import lombok.experimental.UtilityClass;

import java.util.Collection;
import java.util.StringJoiner;

@UtilityClass
public final class StringUtil {

    /**
     * Build up a string out of a separator and a set of other string parts.
     *
     * @param separator the separator.
     * @param parts     the parts to join.
     * @return the built string.
     */
    public String join(String separator, Collection<String> parts) {
        StringJoiner joiner = new StringJoiner(separator);
        for (String part : parts) {
            joiner.add(part);
        }
        return joiner.toString();
    }
}
