
package org.apache.seatunnel.connectors.seatunnel.wanda.source.exception;


import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum WandaConnectorErrorCode implements SeaTunnelErrorCode {
    GET_MYHOURS_TOKEN_FAILE("MYHOURS-01", "Get myhours token failed");

    private final String code;

    private final String description;

    WandaConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getErrorMessage() {
        return SeaTunnelErrorCode.super.getErrorMessage();
    }
}
