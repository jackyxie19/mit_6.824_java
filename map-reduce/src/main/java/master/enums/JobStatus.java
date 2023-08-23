package master.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum JobStatus {
    INITIALED(1),
    SUBMITTED(2),
    RUNNING(3),
    FINISHED(4)

    ;
    int code;
}
