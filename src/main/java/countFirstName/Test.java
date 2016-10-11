package countFirstName;

/**
 * Created by salma on 09/10/2016.
 */
public class Test {

    public static void main(String[] args) throws Exception {
        String value = "wolf;m;german, english, jewish;18.29";
        String origins = value.split(";")[2];
        String[] origins_array = origins.split(",");
        for (String origin : origins_array) {
            System.out.println(origin.trim());
        }
    }
}
