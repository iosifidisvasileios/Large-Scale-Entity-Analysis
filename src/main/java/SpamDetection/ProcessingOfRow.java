package SpamDetection;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import static org.apache.commons.lang3.StringUtils.normalizeSpace;

/**
 * Created by iosifidis on 09.08.16.
 */
public class ProcessingOfRow implements java.io.Serializable{

    public static final HashMap<String,String> slangDictionary = new SlangDictionary().getDictionary();
    public static final HashSet<String> verbs = new VerbLoader().getDictionary();
    public static final HashSet<String> stopwords = new StopwordsLoader().getDictionary();
    public static final HashMap<String, String> adjectives = new OppositeLoader().getDictionary();

    public ProcessingOfRow() {
    }


    public String clearWhitespace(String preprocessed) throws IOException {

        return normalizeSpace(preprocessed.trim());

    }


    public String removeStopwords(String preprocessed) throws IOException {
        String[] s = preprocessed.split(" ");
        String temp = "";

        for(int i =0; i < s.length; i++){
            if(stopwords.contains(s[i])){
                s[i] = "";
            }
        }

        for(int i =0; i < s.length; i++){
            temp += s[i] + " ";
        }
        return temp;

    }

        public String[] tagNegationsAdjective(String s1, String s2) throws IOException {

        final String[] result = new String[2];
        result[0] = s1;
        result[1] = s2;

        if (adjectives.containsKey(s2)){
            result[0] = adjectives.get(s2);
            result[1] = "";
        }
        return result;
    }

    public String negationsBasedOnAdj(String preprocessed) throws IOException {

        String[] strings = preprocessed.split(" ");
        for (int i = 0; i < strings.length; i++) {
            if (strings[i] != null && (strings[i].endsWith("n't") || strings[i].endsWith("not")
                    || strings[i].equals("not") || strings[i].equals("nor") || strings[i].equals("neither"))) {

                if (i + 1 < strings.length && (strings[i + 1].equals("so") || strings[i + 1].equals("very")
                        || strings[i + 1].equals("as") || strings[i + 1].equals("very")
                        || strings[i + 1].equals("such") || strings[i + 1].equals("that")
                        || strings[i + 1].equals("much") || strings[i + 1].equals("the")
                        || strings[i + 1].equals("a")|| strings[i + 1].equals("an"))
                        && i + 2 < strings.length) {
                    String[] temp = tagNegationsAdjective(strings[i], strings[i + 2]);
                    strings[i] = temp[0];
                    strings[i + 1] = "";
                    strings[i + 2] = temp[1];

                } else if (i + 1 < strings.length) {
                    String[] temp = tagNegationsAdjective(strings[i], strings[i + 1]);
                    strings[i] = temp[0];
                    strings[i + 1] = temp[1];
                }
            }
        }
        String s ="";
        for (String str : strings){
            s += str + " ";
        }
        return s;
    }

    public static boolean isVerb(String s) throws IOException {
        if(verbs.contains(s)){
            return true;
        }
        return false;
    }

    public String[] tagNegationsVerbs(String s1, String s2) throws IOException {
        String[] result = new String[2];
        s1 = s1.replace("n't", "not");

        if (isVerb(s2)) {
            s1 = "NOT_";
            s1 = s1 + s2;
            s2 = "";
        }
        result[0] = s1;
        result[1] = s2;
        return result;

    }

    public String negationsBasedOnVerbs(String preprocessed) throws IOException {

        String[] strings = preprocessed.split(" ");
        for (int i = 0; i < strings.length; i++) {
            if (strings[i] != null && (strings[i].endsWith("n't") || strings[i].endsWith("not") || strings[i].equals("nor") || strings[i].equals("neither"))) {
                if (i + 1 < strings.length) {
                    String[] temp = tagNegationsVerbs(strings[i], strings[i + 1]);
                    strings[i] = temp[0];
                    strings[i + 1] = temp[1];
                }
            }
        }
        String s ="";
        for (String str : strings){
            s += str + " ";
        }
        return s;
    }

    public String  removeRepetitions(String s) {
        // dealing with colloqial language
        s = s.replaceAll("a{2,}+", "a");
        s = s.replaceAll("o{3,}+", "o");
        s = s.replaceAll("u{2,}+", "u");
        s = s.replaceAll("i{2,}+", "i");
        s = s.replaceAll("e{3,}+", "e");
        s = s.replaceAll("b{3,}+", "bb");
        s = s.replaceAll("c{3,}+", "cc");
        s = s.replaceAll("d{3,}+", "dd");
        s = s.replaceAll("f{3,}+", "ff");
        s = s.replaceAll("g{3,}+", "gg");
        s = s.replaceAll("h{3,}+", "hh");
        s = s.replaceAll("j{2,}+", "j");
        s = s.replaceAll("k{3,}+", "kk");
        s = s.replaceAll("l{3,}+", "ll");
        s = s.replaceAll("m{3,}+", "mm");
        s = s.replaceAll("n{3,}+", "nn");
        s = s.replaceAll("p{3,}+", "pp");
        s = s.replaceAll("q{3,}+", "qq");
        s = s.replaceAll("r{3,}+", "rr");
        s = s.replaceAll("s{3,}+", "ss");
        s = s.replaceAll("t{3,}+", "tt");
        s = s.replaceAll("v{3,}+", "vv");
        s = s.replaceAll("w{3,}+", "ww");
        s = s.replaceAll("y{2,}+", "y");
        s = s.replaceAll("z{3,}+", "zz");
        return s;
    }

    public String clearText(String text) {
        text  = text.replaceAll("https?://\\S+\\s?", "");
        text  = text.replaceAll("http?://\\S+\\s?", "");
        text  = text.replaceAll("#", "");
        text = text.replaceAll("[.!?_]"," ");
        text  = text.trim();

        if (text.contains("@") || text.contains("＠")) {
            text  = splitAndRemove(text.split(" "));
        }

        text = removeRepetitions(text);
        return text;
    }

    public String slangProcess(String preprocessed) {

        String temp = "";
        String[] s = preprocessed.split(" ");

        for(int i =0; i < s.length; i++){

            if(slangDictionary.containsKey(s[i])){
                s[i] = slangDictionary.get(s[i]);
            }
        }

        for(int i =0; i < s.length; i++){
            temp += s[i] + " ";
        }
        return temp;
    }

    public String removeNonAscii(final String preprocessed) {
//
        String temp = preprocessed;
        temp = temp.replaceAll("[^\\x00-\\x7F]", "");
        temp = temp.replaceAll("[^\\p{L}\\p{Z}&&[^_]]"," ");
        temp = temp.replaceAll("\\b[\\w']{1,2}\\b", "");

        return temp;
    }

    public static String splitAndRemove(String[] split) {
        String output = "";
        for (String item : split){
            if (item.contains("@") || item.contains("＠")) {
                continue;
            }
            output += item +" ";
        }
        return output;
    }


}
