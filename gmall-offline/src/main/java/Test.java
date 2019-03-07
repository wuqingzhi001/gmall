/**
 * @aythor HeartisTiger
 * 2019-03-07 15:33
 */
public class Test {
    public static void main(String[] args) {
        //strtr("abcdaf","ac","AB") == "AbBdAf"
        System.out.println(strtr("abcdaf", "ac", "AB"));
    }
    public static String strtr(String src,String from,String to){
        char[] fromChars = from.toCharArray();
        char[] toChars = to.toCharArray();

        for (int i = 0; i < fromChars.length; i++) {
            src = src.replaceAll(String.valueOf(fromChars[i]),String.valueOf(toChars[i]));
        }


        return src;
    }
}
