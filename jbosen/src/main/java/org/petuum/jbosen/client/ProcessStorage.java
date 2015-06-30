package org.petuum.jbosen.client;

interface ProcessStorage {
    ClientRow getRow(int rowId);

    void putRow(int rowId, ClientRow row);
}