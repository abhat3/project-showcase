package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.net.Uri;
import android.content.Context;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 *
 * Please read:
 *
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 *
 * before you start to get yourself familiarized with ContentProvider.
 *
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 *
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {
    static final String TAG = GroupMessengerProvider.class.getSimpleName();
    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.groupmessenger2.provider";
    static final String URL = "content://" + PROVIDER_NAME;
    static final Uri CONTENT_URI = Uri.parse(URL);

    private SQLiteDatabase db;
    static final String DATABASE_NAME = "SimpleDB";
    static final String TABLE_NAME = "myStore";
    static final String KEY_COLUMN = "key";
    static final String VALUE_COLUMN = "value";

    private static final String SQL_CREATE_ENTRIES = " CREATE TABLE " + TABLE_NAME +
            " ( \"" + KEY_COLUMN +"\" TEXT NOT NULL UNIQUE ON CONFLICT REPLACE, " +
            VALUE_COLUMN + " TEXT NOT NULL);";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + TABLE_NAME;

    private static class DatabaseHandler extends SQLiteOpenHelper {

        DatabaseHandler(Context context){
            super(context, DATABASE_NAME, null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {

            db.execSQL(SQL_CREATE_ENTRIES);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL(SQL_DELETE_ENTRIES);
            onCreate(db);
        }
    }

    private DatabaseHandler dbHandler;

    private void dbInit() {
        if (db == null) {
            if (dbHandler != null) {
                db = dbHandler.getWritableDatabase();
            }
            else {
                Context context = getContext();
                dbHandler = new DatabaseHandler(context);
            }
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

        dbInit();

        long rowID = db.insert(TABLE_NAME, null, values);

        if(rowID < 0) {
            throw new SQLException("Could not add record to database!");
        }
        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        dbInit();
        return true;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        dbInit();

        String[] keyValueToSearch = {selection};
        selection = "\"" + KEY_COLUMN + "\"" + " = ?";

        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        Cursor cursor = queryBuilder.query(db, null, selection, keyValueToSearch, null, null, null);

        if(cursor == null){
            Log.i(TAG, "query: Cursor is null");
        }
        return cursor;
    }
}
