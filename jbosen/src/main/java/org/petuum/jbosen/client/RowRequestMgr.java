package org.petuum.jbosen.client;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

class RowRequestMgr {

    private Table<Integer, Integer, LinkedList<RowRequestInfo>> pendingRowRequests;

    public RowRequestMgr() {
        pendingRowRequests = HashBasedTable.create();
    }

    public boolean addRowRequest(RowRequestInfo request, int tableId, int rowId) {
        request.sent = true;
        if (!pendingRowRequests.contains(tableId, rowId)) {
            pendingRowRequests.put(tableId, rowId,
                    new LinkedList<RowRequestInfo>());
        }
        LinkedList<RowRequestInfo> requestList = pendingRowRequests.get(
                tableId, rowId);
        boolean requestAdded = false;
        // Requests are sorted in increasing order of clock number.
        // When a request is to be inserted, start from the end as the request's
        // clock is more likely to be larger.
        ListIterator<RowRequestInfo> iter = requestList
                .listIterator(requestList.size());
        int clock = request.clock;
        while (iter.hasPrevious()) {
            RowRequestInfo prev = iter.previous();
            if (clock >= prev.clock) {
                request.sent = false;
                iter.next();
                iter.add(request);
                requestAdded = true;
                break;
            }
        }
        if (!requestAdded) {
            requestList.addFirst(request);
        }

        return request.sent;
    }

    public int informReply(int tableId, int rowId, int clock, int currVersion,
                           List<Integer> appThreadIds) {
        appThreadIds.clear();
        LinkedList<RowRequestInfo> requestList = pendingRowRequests.get(
                tableId, rowId);
        int clockToRequest = -1;

        boolean satisfiedSent = false;
        while (requestList != null && !requestList.isEmpty()) {
            RowRequestInfo request = requestList.getFirst();
            if (request.clock <= clock && !satisfiedSent) {
                // remove the request
                appThreadIds.add(request.appThreadId);
                requestList.pollFirst();

                if (request.sent) {
                    satisfiedSent = true;
                }
            } else {
                if (!request.sent) {
                    clockToRequest = request.clock;
                    request.sent = true;
                    request.version = currVersion - 1;
                }
                break;
            }
        }
        // if there's no request in that list, I can remove the empty list
        if (requestList != null && requestList.isEmpty())
            pendingRowRequests.remove(tableId, rowId);
        return clockToRequest;
    }

}
