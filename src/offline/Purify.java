package offline;

import java.io.BufferedReader;
import java.io.FileReader;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

import db.mongodb.MongoDBUtil;

public class Purify {
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase(MongoDBUtil.DB_NAME);
		// The name of the file to open.
		// Windows is different : C:\\Documents\\ratings_Musical_Instruments.csv
		//String fileName = "/Users/project/Downloads/ratings_Musical_Instruments.csv";
		String fileName = "C:\\Users\\zhang\\Downloads\\ratings_Musical_Instruments.csv";
		
		String line = null;

		try {
			FileReader fileReader = new FileReader(fileName);

			BufferedReader bufferedReader = new BufferedReader(fileReader);
			while ((line = bufferedReader.readLine()) != null) {
				String[] values = line.split(",");

				//这个可以加上去重复的代码，防止重新输入相同的内容
				//08/08/2017
//				UpdateOptions options = new UpdateOptions().upsert(true);
//				db.getCollection("ratings").updateOne(
//						new Document().append("user", values[0]).append("item", values[1]),
//						new Document("$set", new Document().append("rating", Double.parseDouble(values[2]))), options);
				
				db.getCollection("ratings")
						.insertOne(
								new Document()
										.append("user", values[0])
										.append("item", values[1])
										.append("rating",
												Double.parseDouble(values[2])));

			}
			System.out.println("Import Done!");
			bufferedReader.close();
			mongoClient.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

