package net.escosoft.mysqlwrapper.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class Preconditions {

    /**
     * Checks whether and object is null and throw an exception if it is.
     *
     * @param obj     the object to check.
     * @param message the message to print if there's an exception.
     * @return the given object if passed.
     */
    public <T> T checkNonNull(T obj, String message) {
        if (obj == null) {
            throw new NullPointerException(message);
        }
        return obj;
    }

    /**
     * Checks if the length of an array is at least at a minimum.
     *
     * @param array   the array to check.
     * @param min     the minimum length.
     * @param message the message to print if there's an exception.
     */
    public <T> void checkLength(T[] array, int min, String message) {
        if (array.length < min) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks if a number is between a range of numbers.
     *
     * @param value   the value to check.
     * @param min     the minimum bound.
     * @param max     the maximum bound.
     * @param message the message to print if there's an exception.
     */
    public void checkRange(int value, int min, int max, String message) {
        if (value < min || value > max) {
            throw new IndexOutOfBoundsException(message);
        }
    }
}
