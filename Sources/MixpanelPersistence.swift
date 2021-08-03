//
//  MixpanelPersistence.swift
//  Mixpanel
//
//  Created by ZIHE JIA on 7/9/21.
//  Copyright © 2021 Mixpanel. All rights reserved.
//

import Foundation

enum PersistenceType: String, CaseIterable {
    case events = "events"
    case people = "people"
    case groups = "groups"
    case properties = "properties"
    case optOutStatus = "optOutStatus"
}


class MixpanelPersistence {
    
    static let sharedInstance: MixpanelPersistence = {
        let instance = MixpanelPersistence()
        
        
        return instance
    }()
    
    
    
    func saveEntity(_ entity: InternalProperties, type: PersistenceType) {
        if let data = JSONHandler.serializeJSONObject(entity) {
            MPDB.insertRow(type, data: data)
        }
    }
    
    func saveEntities(_ entities: Queue, type: PersistenceType) {
        for entity in entities {
            if let data = JSONHandler.serializeJSONObject(entity) {
                MPDB.insertRow(type, data: data)
            }
        }
    }
    
    func loadEntity(_ type: PersistenceType) -> InternalProperties? {
        let jsonArray : [InternalProperties] = loadEntitiesInBatch(1, type: type)
        if !jsonArray.isEmpty {
            return jsonArray[0]
        }
        return nil
    }
    
    func loadEntitiesInBatch(_ batchSize: Int = 50, type: PersistenceType) -> Queue {
        let dataArray = MPDB.readRows(type, numRows: batchSize)
        var jsonArrary : Queue = []
        for entity in dataArray {
            if let jsonObject = JSONHandler.deserializeData(entity) as? InternalProperties {
                jsonArrary.append(jsonObject)
            }
        }
        return jsonArrary
    }
    
    func removeEntitiesInBatch(_ batchSize: Int = 50, type: PersistenceType) {
        MPDB.deleteRows(type, numRows: batchSize)
    }
    
    func resetEntities() {
        for pType in PersistenceType.allCases {
            MPDB.deleteRows(pType, numRows: Int.max)
        }
    }
    
}
