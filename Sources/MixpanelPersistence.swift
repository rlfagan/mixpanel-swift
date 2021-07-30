//
//  MixpanelPersistence.swift
//  Mixpanel
//
//  Created by ZIHE JIA on 7/9/21.
//  Copyright © 2021 Mixpanel. All rights reserved.
//

import Foundation

enum PersistenceType: String {
    case events
    case people
    case groups
    case properties
    case optOutStatus
}


class MixpanelPersistence {
    
    static let sharedInstance: MixpanelPersistence = {
        let instance = MixpanelPersistence()
        
        
        return instance
    }()
    
    
    
    func saveEntity(_ entity: InternalProperties, type: PersistenceType, token: String) {
        if let data = JSONHandler.serializeJSONObject(entity) {
            MPDB.insertRow(type, data: data)
        }
    }
    
    func saveEntities(_ entities: Queue, type: PersistenceType, token: String) {
        
    }
    
    func loadEntity(_ type: PersistenceType, token: String) -> InternalProperties? {
        let jsonArray : [InternalProperties] = loadEntitiesInBatch(1, type: type, token: token)
        if !jsonArray.isEmpty {
            return jsonArray[0]
        }
        return nil
    }
    
    func loadEntitiesInBatch(_ batchSize: Int = 50, type: PersistenceType, token: String) -> Queue {
        var jsonArrary : [InternalProperties] = []
        let dataArray = MPDB.readRows(type, numRows: batchSize)
        for entity in dataArray {
            if let jsonObject = JSONHandler.deserializeData(entity) as? InternalProperties {
                jsonArrary.append(jsonObject)
            }
        }
        return jsonArrary
    }
    
    func removeEventsInBatch(_ batchSize: Int = 50, type: PersistenceType, token: String) {
        MPDB.deleteRows(type, numRows: batchSize)
    }
    
    func resetEvents() {
        
    }
    
}
