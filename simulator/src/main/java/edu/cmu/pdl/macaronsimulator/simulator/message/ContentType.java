package edu.cmu.pdl.macaronsimulator.simulator.message;

/**
 * Message's content types.
 * - OBJECT:    If the content type of a message is OBJECT, this message is a data request/response type (GET, PUT, DELETE)
 * - METADATA:  If the content type of a message is METADATA, this message includes metadata of the data (currently, 
 *              only used in the messages sent/recv to/from Object Storage Metadata Server)
 */
public enum ContentType {
    OBJECT, METADATA, CLUSTER_INFO
}
