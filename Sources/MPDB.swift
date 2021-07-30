//
//  MPDB.swift
//  Mixpanel
//
//  Created by Jared McFarland on 7/2/21.
//  Copyright © 2021 Mixpanel. All rights reserved.
//

import Foundation
import SQLite3

class MPDB {
    private static var connection: OpaquePointer?
    private static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    
    private static let DB_FILE_NAME: String = "MPDB.sqlite"
    private static let NO_DB_CONNECTION: String = "No database connection found. Calling MPDB.open()"
    
    private static func pathToDb() -> String? {
        let manager = FileManager.default
        #if os(iOS)
            let url = manager.urls(for: .libraryDirectory, in: .userDomainMask).last
        #else
            let url = manager.urls(for: .cachesDirectory, in: .userDomainMask).last
        #endif // os(iOS)

        guard let urlUnwrapped = url?.appendingPathComponent(DB_FILE_NAME).path else {
            return nil
        }
        return urlUnwrapped
    }
    
    static private func tableNameFor(_ persistenceType: PersistenceType, token: String) -> String {
        return "mixpanel_\(token)_\(persistenceType)"
    }
    
    static func open() {
        if let dbPath = pathToDb() {
            if sqlite3_open_v2(dbPath, &connection, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) != SQLITE_OK {
                logError(message: "Error opening database at path: \(dbPath)")
                close()
            }
            else {
                Logger.info(message: "Successfully opened connection to database at path: \(dbPath)")
            }
        }
    }
    
    static func close() {
        sqlite3_close(connection)
        Logger.info(message: "Connection to database closed.")
    }
    
    static private func recreateDb() {
        close()
        if let dbPath = pathToDb() {
            do {
                let manager = FileManager.default
                if manager.fileExists(atPath: dbPath) {
                    try manager.removeItem(atPath: dbPath)
                    Logger.info(message: "Deleted database file at path: \(dbPath)")
                }
            }
            catch let error {
                Logger.error(message: "Unable to remove database file at path: \(dbPath), error: \(error)")
            }
        }
        open()
    }
    
    static func createTable(_ persistenceType: PersistenceType, token: String) {
        if let db = connection {
            let tableName = tableNameFor(persistenceType, token: token)
            let createTableString = "CREATE TABLE IF NOT EXISTS \(tableName)(id integer primary key autoincrement,data blob,time real);"
            var createTableStatement: OpaquePointer? = nil
            if sqlite3_prepare_v2(db, createTableString, -1, &createTableStatement, nil) == SQLITE_OK {
                if sqlite3_step(createTableStatement) == SQLITE_DONE {
                    Logger.info(message: "\(tableName) table created")
                } else {
                    logError(message: "\(tableName) table create failed")
                    recreateDb()
                }
            } else {
                logError(message: "CREATE statement for table \(tableName) could not be prepared")
                recreateDb()
            }
            sqlite3_finalize(createTableStatement)
        } else {
            Logger.warn(message: NO_DB_CONNECTION)
            open()
        }
    }
    
    static func insertRow(_ persistenceType: PersistenceType, token: String, data: Data) {
        if let db = connection {
            let tableName = tableNameFor(persistenceType, token: token)
            let insertString = "INSERT INTO \(tableName) (data, time) VALUES(?, ?);"
            var insertStatement: OpaquePointer? = nil
            data.withUnsafeBytes { rawBuffer in
                if let pointer = rawBuffer.baseAddress {
                    if sqlite3_prepare_v2(db, insertString, -1, &insertStatement, nil) == SQLITE_OK {
                        sqlite3_bind_blob(insertStatement, 1, pointer, Int32(rawBuffer.count), SQLITE_TRANSIENT)
                        sqlite3_bind_double(insertStatement, 2, Date().timeIntervalSince1970)
                        if sqlite3_step(insertStatement) == SQLITE_DONE {
                            Logger.info(message: "Successfully inserted row into table \(tableName)")
                        } else {
                            logError(message: "Failed to insert row into table \(tableName)")
                            recreateDb()
                        }
                    } else {
                        logError(message: "INSERT statement for table \(tableName) could not be prepared")
                        recreateDb()
                    }
                    sqlite3_finalize(insertStatement)
                }
            }
        } else {
            Logger.warn(message: NO_DB_CONNECTION)
            open()
        }
    }
    
    static func deleteRows(_ persistenceType: PersistenceType, token: String, numRows: Int) {
        if let db = connection {
            let tableName = tableNameFor(persistenceType, token: token)
            let deleteString = "DELETE FROM \(tableName) WHERE id IN (SELECT id FROM \(tableName) ORDER BY time LIMIT \(numRows))"
            var deleteStatement: OpaquePointer? = nil
            if sqlite3_prepare_v2(db, deleteString, -1, &deleteStatement, nil) == SQLITE_OK {
                if sqlite3_step(deleteStatement) == SQLITE_DONE {
                    Logger.info(message: "Succesfully deleted rows from table \(tableName)")
                } else {
                    logError(message: "Failed to delete rows from table \(tableName)")
                    recreateDb()
                }
            } else {
                logError(message: "DELETE statement for table \(tableName) could not be prepared")
                recreateDb()
            }
            sqlite3_finalize(deleteStatement)
        } else {
            Logger.warn(message: NO_DB_CONNECTION)
            open()
        }
    }
    
    static func readRows(_ persistenceType: PersistenceType, token: String, numRows: Int)  -> [Data] {
        var rows: [Data] = []
        if let db = connection {
            let tableName = tableNameFor(persistenceType, token: token)
            let selectString = "SELECT data FROM \(tableName) WHERE id IN (SELECT id FROM \(tableName) ORDER BY time LIMIT \(numRows))"
            var selectStatement: OpaquePointer? = nil
            var rowsRead: Int = 0
            if sqlite3_prepare_v2(db, selectString, -1, &selectStatement, nil) == SQLITE_OK {
                while sqlite3_step(selectStatement) == SQLITE_ROW {
                    if let blob = sqlite3_column_blob(selectStatement, 0) {
                        let blobLength = sqlite3_column_bytes(selectStatement, 0)
                        let data = Data(bytes: blob, count: Int(blobLength))
                        rows.append(data)
                        rowsRead += 1
                    } else {
                        logError(message: "No blob found in data column for row in \(tableName)")
                    }
                }
                Logger.info(message: "Successfully read \(rowsRead) from table \(tableName)")
            } else {
                logError(message: "SELECT statement for table \(tableName) could not be prepared")
            }
            sqlite3_finalize(selectStatement)
        } else {
            Logger.warn(message: NO_DB_CONNECTION)
            open()
        }
        return rows
    }
    
    static private func logError(message: String? = nil) {
        if let db = connection {
            if let msg = message {
                Logger.error(message: msg)
            }
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            Logger.error(message: errmsg)
        }
    }
}
