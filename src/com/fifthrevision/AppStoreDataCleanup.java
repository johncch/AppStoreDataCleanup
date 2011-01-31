package com.fifthrevision;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import processing.core.PApplet;
import processing.core.PImage;
import toxi.color.HistEntry;
import toxi.color.Histogram;
import toxi.color.TColor;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteConstants;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class AppStoreDataCleanup extends PApplet {
	
	private static final long serialVersionUID = 1L;
	
	public static String OUT_DB_NAME = "iTunesAppStore.db";
	public static String DB_FOLDER = "db/";
	public static String IMG_FOLDER = "images/";
	public static String GENRE_FILENAME = "allGenres.txt";

	public static String filepath = "/Users/johncch/Documents/Projects/AppStoreScraperJS2/";
	
	public static void main(String[] args) {
		PApplet.main(new String[] { "com.fifthrevision.AppStoreDataCleanup" });
		
	};
	
	private float tolerance = 0.2f;
	
	@Override
	public void setup() {
		File genresFile = new File(filepath + GENRE_FILENAME);
		BufferedReader reader = null;
		ArrayList<String> genreList = new ArrayList<String>();
		// String[] genres = null;
		
		try {
			reader = new BufferedReader(new FileReader(genresFile));
			String line;
			while((line = reader.readLine()) != null) {
				String[] lineSeg = line.split(" ");
				if(lineSeg.length == 2) {
					genreList.add(lineSeg[0]);
				}
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		SQLiteConnection db = new SQLiteConnection(new File(filepath + DB_FOLDER + OUT_DB_NAME));
		
		try {
			db.open(true);
			SQLiteStatement st = db.prepare("CREATE TABLE IF NOT EXISTS entries(id integer primary key, url text, name text, price real, genre text, category text, released integer, version text, size real, seller text, language text, currRating integer, allRating integer, numCurrRating integer, numAllRating integer, appRating integer, colorOne text, colorOneF real, colorTwo text, colorTwoF real, colorThree text, colorThreeF real)");
			while(st.step()) {
				System.out.println("Stepped!");
			}
		} catch (SQLiteException e) {
			e.printStackTrace();
		}
		
		SQLiteConnection readDb = null;		
		SQLiteStatement insertSt = null;
		SQLiteStatement selectSt = null;
		
		int count = 0;
		
		for(int i = 0; i < genreList.size(); i++) {
			String genre = genreList.get(i);
			try {
				readDb = new SQLiteConnection(new File(filepath + DB_FOLDER + genre + ".db"));
				readDb.openReadonly();
				SQLiteStatement st = readDb.prepare("SELECT * FROM entries");
				while (st.step()) {	
					selectSt = db.prepare("SELECT * FROM entries WHERE id = ?");
					selectSt.bind(1, st.columnString(0));
					if(selectSt.step()) {
						System.out.println("Selecting " + st.columnString(0) + ", skipping..");
						selectSt.dispose();
						count++;
						if(count % 1000 == 0) {
							System.out.println("Entry " + count);
						}
						continue;
					}
					selectSt.dispose();
					
					insertSt = db.prepare("INSERT INTO entries (genre, id, url, name, price, category, released, version, size, seller, language, currRating, allRating, numCurrRating, numAllRating, appRating, colorOne, colorOneF, colorTwo, colorTwoF, colorThree, colorThreeF) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

					insertSt.bind(1, genre);
					int k = 1;
					for(k = 1; k <= st.columnCount(); k++) {
						if(st.columnType(k - 1) == SQLiteConstants.SQLITE_INTEGER) {
							insertSt.bind(k + 1, st.columnLong(k - 1));
						} else if(st.columnType(k - 1) == SQLiteConstants.SQLITE_TEXT) {
							insertSt.bind(k + 1, st.columnString(k - 1));
						} else if(st.columnType(k - 1) == SQLiteConstants.SQLITE_FLOAT) {
							insertSt.bind(k + 1, st.columnDouble(k - 1));
						}
					}
					
					PImage img = loadImage(filepath + IMG_FOLDER + st.columnString(0) + ".jpg");
					if(img != null) {
						Histogram hist = Histogram.newFromARGBArray(img.pixels, img.pixels.length / 10, tolerance, true);
						int j = 0;
						for(Iterator<HistEntry> it = hist.iterator(); it.hasNext() && j < 3; j++) {
							HistEntry en = it.next();
							TColor color = en.getColor();
							insertSt.bind(++k, color.toHex());
							insertSt.bind(++k, en.getFrequency());
							
						}
						insertSt.step();
						count++;
						if(count % 1000 == 0) {
							System.out.println("Entry " + count);
						}
					}
					insertSt.dispose();
				}
				st.dispose();
			} catch (SQLiteException e) {
				e.printStackTrace();
			} finally {
				readDb.dispose();
			}
			System.out.println("Finished " + genreList.get(i));
		}
		
		db.dispose();
	}
	
}
