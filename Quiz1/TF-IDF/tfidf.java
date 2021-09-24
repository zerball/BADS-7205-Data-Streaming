import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class tfidf{

    private static Pattern twopart = Pattern.compile("(\\d+)-(\\d+)");

    public static void checkString(String s)
    {
        // Matcher m = twopart.matcher(s);
        // if (m.matches()) {
        //     System.out.println(s + " matches; first part is " + m.group(1) +
        //                        ", second part is " + m.group(2) + ".");
        // } else {
        //     System.out.println(s + " does not match.");
        // }
    }

    public static void main(String[] args) {
        String string = "It was the best of times, it was the worst of times,\n"
        + "it was the age of wisdom, it was the age of foolishness,\n"
        + "Page 2 | it was the epoch of belief, it was the epoch of incredulity,\n"
        + "it was the season of Light, it was the season of Darkness,\n"
        + "Page 3 | it was the spring of hope, it was the winter of despair,\n"
        + "we had everything before us, we had nothing before us";
        String[] parts = string.split("Page");
        System.out.println(parts);
        String part1 = parts[0];
        String part2 = parts[1];
        System.out.println(lenght(parts));
        System.out.println(part1);
        System.out.println("****************************");
        System.out.print(part2);
    }
}

// String lines[] = string.split("\\r?\\n");

