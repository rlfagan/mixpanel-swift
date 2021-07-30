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
    private static var token: String?
    private static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    private static let DB_FILE_NAME: String = "MPDB.sqlite"
    
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
    
    private static func tableNameFor(_ persistenceType: PersistenceType) -> String {
        return "mixpanel_\(token!)_\(persistenceType)"
    }
    
    private static func reconnect() {
        Logger.warn(message: "No database connection found. Calling MPDB.open()")
        if let projectToken = token {
            open(projectToken)
        } else {
            Logger.warn(message: "No project token found. Database cannot be opened. Make sure you've called MPDB.open(projectToken)")
        }
    }
    
    static func open(_ projectToken: String) {
        if !projectToken.isEmpty {
            token = projectToken
        } else {
            Logger.error(message: "Project token must not be empty. Database cannot be opened.")
            return
        }
        if let dbPath = pathToDb() {
            if sqlite3_open_v2(dbPath, &connection, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) != SQLITE_OK {
                logSqlError(message: "Error opening or creating database at path: \(dbPath)")
                close()
            }
            else {
                Logger.info(message: "Successfully opened connection to database at path: \(dbPath)")
                createTables()
            }
        }
    }
    
    static func close() {
        sqlite3_close(connection)
        Logger.info(message: "Connection to database closed.")
    }
    
    static private func recreate() {
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
        reconnect()
    }
    
    static private func createTableFor(_ persistenceType: PersistenceType) {
        if let db = connection {
            let tableName = tableNameFor(persistenceType)
            let createTableString = "CREATE TABLE IF NOT EXISTS \(tableName)(id integer primary key autoincrement,data blob,time real);"
            var createTableStatement: OpaquePointer? = nil
            if sqlite3_prepare_v2(db, createTableString, -1, &createTableStatement, nil) == SQLITE_OK {
                if sqlite3_step(createTableStatement) == SQLITE_DONE {
                    Logger.info(message: "\(tableName) table created")
                } else {
                    logSqlError(message: "\(tableName) table create failed")
                    recreate()
                }
            } else {
                logSqlError(message: "CREATE statement for table \(tableName) could not be prepared")
                recreate()
            }
            sqlite3_finalize(createTableStatement)
        } else {
            reconnect()
        }
    }
    
    static private func createTables() {
        createTableFor(PersistenceType.events)
        createTableFor(PersistenceType.people)
        createTableFor(PersistenceType.groups)
        createTableFor(PersistenceType.properties)
        createTableFor(PersistenceType.optOutStatus)
    }
    
    static func insertRow(_ persistenceType: PersistenceType, data: Data) {
        if let db = connection {
            let tableName = tableNameFor(persistenceType)
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
                            logSqlError(message: "Failed to insert row into table \(tableName)")
                            recreate()
                        }
                    } else {
                        logSqlError(message: "INSERT statement for table \(tableName) could not be prepared")
                        recreate()
                    }
                    sqlite3_finalize(insertStatement)
                }
            }
        } else {
            reconnect()
        }
    }
    
    static func deleteRows(_ persistenceType: PersistenceType, numRows: Int) {
        if let db = connection {
            let tableName = tableNameFor(persistenceType)
            let deleteString = "DELETE FROM \(tableName) WHERE id IN (SELECT id FROM \(tableName) ORDER BY time LIMIT \(numRows))"
            var deleteStatement: OpaquePointer? = nil
            if sqlite3_prepare_v2(db, deleteString, -1, &deleteStatement, nil) == SQLITE_OK {
                if sqlite3_step(deleteStatement) == SQLITE_DONE {
                    Logger.info(message: "Succesfully deleted rows from table \(tableName)")
                } else {
                    logSqlError(message: "Failed to delete rows from table \(tableName)")
                    recreate()
                }
            } else {
                logSqlError(message: "DELETE statement for table \(tableName) could not be prepared")
                recreate()
            }
            sqlite3_finalize(deleteStatement)
        } else {
            reconnect()
        }
    }
    
    static func readRows(_ persistenceType: PersistenceType, numRows: Int)  -> [Data] {
        var rows: [Data] = []
        if let db = connection {
            let tableName = tableNameFor(persistenceType)
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
                        logSqlError(message: "No blob found in data column for row in \(tableName)")
                    }
                }
                Logger.info(message: "Successfully read \(rowsRead) from table \(tableName)")
            } else {
                logSqlError(message: "SELECT statement for table \(tableName) could not be prepared")
            }
            sqlite3_finalize(selectStatement)
        } else {
            reconnect()
        }
        return rows
    }
    
    static private func logSqlError(message: String? = nil) {
        if let db = connection {
            if let msg = message {
                Logger.error(message: msg)
            }
            let sqlError = String(cString: sqlite3_errmsg(db)!)
            Logger.error(message: sqlError)
        } else {
            reconnect()
        }
    }
}
