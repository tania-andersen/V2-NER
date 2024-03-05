// Eksempel til Version2-artikel https://www.version2.dk/node/6996719

package jobtrends.analysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Random;

public class TermCandidateFinder {
    private static final String URL = "https://api.openai.com/v1/chat/completions";
    private static final String API_KEY = "<OPENAI-API-NØGLE>";
    private static final String MODEL = "gpt-4";

    public static final String CONNECTION_STRING = "jdbc:derby:db/jobtrendsDB";
    private static Connection connection = null;

    public synchronized static Connection getConnection() {
        if (connection == null) {
            try {
                connection = DriverManager.getConnection(CONNECTION_STRING);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return connection;
    }

    public static String processText(String text) {
        String prompt = String.format("What are the TECHNICAL qualifications and TECHNICAL terms mentioned" +
                " in this job add? VERY IMPORTANT: Answer as a comma-separated list. NO OTHER TEXT. ALSO: Try to use" +
                " only 1 or 2 words for each item. Here is the job ad: \"\"\"%s\"\"\"", text);

        return serviceResponse(prompt);
    }

    public static int getRowCount(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS \"rowcount\" FROM \"APP\".\"ads\"");
        rs.next();
        int count = rs.getInt("rowcount");
        rs.close();
        return count;
    }

    public static HashSet<Integer> generateRandomNumbers(int count) {
        HashSet<Integer> randomNumbers = new HashSet<>();
        Random rand = new Random();
        while (randomNumbers.size() < 20) {
            int randomRow = rand.nextInt(count);
            randomNumbers.add(randomRow);
        }
        return randomNumbers;
    }

    public static void writeAdsToCSV(PreparedStatement selectText, HashSet<Integer> randomNumbers) throws SQLException, IOException {
        FileWriter writer = new FileWriter("output.csv");
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Ad ID", "Text", "Processed Text"));

        for (int randomRow : randomNumbers) {
            selectText.setInt(1, randomRow);
            ResultSet rs = selectText.executeQuery();
            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");

                System.out.println("Ad: " + text);
                String processedText = processText(text);

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                csvPrinter.printRecord(id, text, processedText);
            }
            rs.close();
        }

        csvPrinter.flush();
        csvPrinter.close();
    }

    public static void processAds() {
        try {
            Connection conn = getConnection();
            Statement stmt = conn.createStatement();

            int rowCount = getRowCount(stmt);
            HashSet<Integer> randomNumbers = generateRandomNumbers(rowCount);

            PreparedStatement selectText = conn.prepareStatement("SELECT \"id\", \"text\" FROM \"APP\".\"ads\" OFFSET ? ROWS FETCH NEXT 1 ROW ONLY");
            writeAdsToCSV(selectText, randomNumbers);

            selectText.close();
            stmt.close();
            conn.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    public static String serviceResponse(String prompt) {
        prompt = escapeQuotesForJson(prompt);
        HttpURLConnection connection = null;
        try {
            var obj = new URL(URL);
            connection = (HttpURLConnection) obj.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Bearer " + API_KEY);
            connection.setRequestProperty("Content-Type", "application/json");
            var body = "{\"model\": \"" + MODEL + "\", \"messages\": [{\"role\": \"user\", \"content\": \"" + prompt + "\"}]}";
            connection.setDoOutput(true);
            var writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(body);
            writer.flush();
            writer.close();
            BufferedReader br;
            if (connection.getResponseCode() == 200) {
                br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            } else {
                br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            }
            var line = "";
            var response = new StringBuffer();
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            br.close();
            return extractMessageFromJSONResponse(response.toString());
        } catch (IOException e) {
            if (connection != null) {
                try {
                    var br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                    var line = "";
                    var errorResponse = new StringBuffer();
                    while ((line = br.readLine()) != null) {
                        errorResponse.append(line);
                    }
                    br.close();
                    return errorResponse.toString();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static String escapeQuotesForJson(String str) {
        return replaceLineBreaks(str).replace("\"", "\\\"");
    }

    public static String replaceLineBreaks(String text) {
        return text.replace("\r\n", "\\\\n").replace("\n", "\\\\n").replace("\r", "\\\\n");
    }

    public static String extractMessageFromJSONResponse(String response) {
        System.out.println("Response: " + response);
        return response;
    }

    public static void main(String[] args) {
        processAds();
    }
}
