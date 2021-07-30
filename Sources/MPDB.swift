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
    
    static private func tableNameFor(_ persistenceType: PersistenceType, token: String) -> String {
        return "mixpanel_\(token)_\(persistenceType)"
    }
    
    static func open() -> OpaquePointer? {
        let manager = FileManager.default
        #if os(iOS)
            let url = manager.urls(for: .libraryDirectory, in: .userDomainMask).last
        #else
            let url = manager.urls(for: .cachesDirectory, in: .userDomainMask).last
        #endif // os(iOS)

        guard let urlUnwrapped = url?.appendingPathComponent("MPDB.sqlite").path else {
            return nil
        }
        if sqlite3_open_v2(urlUnwrapped, &connection, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) != SQLITE_OK {
            logError(message: "Error opening database")
            close()
            return nil
        }
        else {
            Logger.info(message: "Successfully opened connection to database")
            return connection
        }
    }
    
    static func close() {
        sqlite3_close(connection)
    }
    
    static func createTable(_ persistenceType: PersistenceType, token: String) {
        let tableName = tableNameFor(persistenceType, token: token)
        let createTableString = "CREATE TABLE IF NOT EXISTS \(tableName)(id integer primary key autoincrement,data blob,time real);"
        var createTableStatement: OpaquePointer? = nil
        if sqlite3_prepare_v2(connection, createTableString, -1, &createTableStatement, nil) == SQLITE_OK {
            if sqlite3_step(createTableStatement) == SQLITE_DONE {
                Logger.info(message: "\(tableName) table created")
            } else {
                logError(message: "\(tableName) table create failed")
            }
        } else {
            logError(message: "CREATE statement for table \(tableName) could not be prepared")
        }
        sqlite3_finalize(createTableStatement)
    }
    
    static func insertRow(_ persistenceType: PersistenceType, token: String, data: Data) {
        let tableName = tableNameFor(persistenceType, token: token)
        let insertString = "INSERT INTO \(tableName) (data, time) VALUES(?, ?);"
        var insertStatement: OpaquePointer? = nil
        data.withUnsafeBytes { rawBuffer in
            if let pointer = rawBuffer.baseAddress {
                if sqlite3_prepare_v2(connection, insertString, -1, &insertStatement, nil) == SQLITE_OK {
                    sqlite3_bind_blob(insertStatement, 1, pointer, Int32(rawBuffer.count), SQLITE_TRANSIENT)
                    sqlite3_bind_double(insertStatement, 2, Date().timeIntervalSince1970)
                    if sqlite3_step(insertStatement) == SQLITE_DONE {
                        Logger.info(message: "Successfully inserted row.")
                    } else {
                        logError(message: "Failed to insert row into \(tableName)")
                    }
                } else {
                    logError(message: "INSERT statement for table \(tableName) could not be prepared")
                }
                sqlite3_finalize(insertStatement)
            }
        }
    }
    
    static func deleteRows(_ persistenceType: PersistenceType, token: String, numRows: Int) {
        let tableName = tableNameFor(persistenceType, token: token)
        let deleteString = "DELETE FROM \(tableName) WHERE id IN (SELECT id FROM \(tableName) ORDER BY time LIMIT \(numRows))"
        var deleteStatement: OpaquePointer? = nil
        if sqlite3_prepare_v2(connection, deleteString, -1, &deleteStatement, nil) == SQLITE_OK {
            if sqlite3_step(deleteStatement) == SQLITE_DONE {
                Logger.info(message: "Succesfully deleted \(numRows) rows from \(tableName)")
            } else {
                logError(message: "Failed to delete \(numRows) rows from \(tableName)")
            }
        } else {
            logError(message: "DELETE statement for table \(tableName) could not be prepared")
        }
        sqlite3_finalize(deleteStatement)
    }
    
    static func readRows(_ persistenceType: PersistenceType, token: String, numRows: Int)  -> [Data] {
        let tableName = tableNameFor(persistenceType, token: token)
        var rows: [Data] = []
        let selectString = "SELECT data FROM \(tableName) WHERE id IN (SELECT id FROM \(tableName) ORDER BY time LIMIT \(numRows))"
        var selectStatement: OpaquePointer? = nil
        if sqlite3_prepare_v2(connection, selectString, -1, &selectStatement, nil) == SQLITE_OK {
            while sqlite3_step(selectStatement) == SQLITE_ROW {
                if let blob = sqlite3_column_blob(selectStatement, 0) {
                    let blobLength = sqlite3_column_bytes(selectStatement, 0)
                    let data = Data(bytes: blob, count: Int(blobLength))
                    rows.append(data)
                } else {
                    logError(message: "No blob found in data column for row in \(tableName)")
                }
            }
        } else {
            logError(message: "SELECT statement for table \(tableName) could not be prepared")
        }
        sqlite3_finalize(selectStatement)
        return rows
    }
    
    static private func logError(message: String? = nil) {
        if let database = connection {
            if let msg = message {
                Logger.error(message: msg)
            }
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            Logger.error(message: errmsg)
        }
    }
}
