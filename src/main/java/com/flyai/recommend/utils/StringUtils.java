package com.flyai.recommend.utils;

/**
 * @author lizhe
 */
public class StringUtils {
    public static final char UNDERLINE = '_';

    public static String camelToUnderline(String param, String remove, String addTo) {
        if (param == null || "".equals(param.trim())) {
            return "";
        }
        int len = param.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = param.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append(UNDERLINE);
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return extra(sb.toString(), addTo != null ? UNDERLINE + addTo : null, remove != null ? UNDERLINE + remove : null);
    }

    public static String underlineToCamel(String param, String remove, String addTo) {
        if (param == null || "".equals(param.trim())) {
            return "";
        }
        int len = param.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = param.charAt(i);
            if (i == 0) {
                c = Character.toUpperCase(c);
            }
            if (c == UNDERLINE) {
                if (++i < len) {
                    sb.append(Character.toUpperCase(param.charAt(i)));
                }
            } else {
                sb.append(c);
            }
        }
        return extra(sb.toString(), addTo, remove);
    }

    private static String extra(String result, String addTo, String remove) {
        if (result.startsWith("_")) {
            result = result.substring(1);
        }
        if (result.endsWith("_")) {
            result = result.substring(0, result.length() - 1);
        }
        if (addTo != null) {
            result += addTo;
        }
        if (remove != null) {
            result = result.replace(remove, "");
        }
        return result;
    }
}
