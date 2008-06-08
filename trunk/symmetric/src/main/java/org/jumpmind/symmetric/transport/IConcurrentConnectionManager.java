package org.jumpmind.symmetric.transport;

public interface IConcurrentConnectionManager {

    public static enum ReservationType {
        
        /**
         * A hard reservation is one that is expected to be released. It does
         * not have a timeout.
         */
        HARD,
        
        /**
         * A soft reservation is one that will time out eventually.
         */
        SOFT
        
    };

    /**
     * @param nodeId
     * @param reservationRequest
     *                if true then hold onto reservation for the time it
     *                typically takes for a node to reconnect after the initial
     *                request. Otherwise, we know that the node has actually
     *                connected for activity.
     * @return true if the connection has been reserved and the node is meant to
     *         proceed with its current operation.
     */
    public boolean reserveConnection(String nodeId, String poolId, ReservationType reservationRequest);

    public boolean releaseConnection(String nodeId, String poolId);
    
    public int getReservationCount(String poolId);

}
