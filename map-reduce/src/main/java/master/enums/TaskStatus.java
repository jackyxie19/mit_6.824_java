package master.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum TaskStatus {
    INITIALED(1),
    SUBMITTED(2),
    RUNNING(3),
    FINISHED(4);
    final int code;
}
