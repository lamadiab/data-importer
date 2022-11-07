package eu.topi.dataimporter.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class CustomerRecord {
    String ref;

    String name;

    String email;

    String address;

    String countryCode;

    @JsonIgnore
    public boolean isValid() {
        return !(this.ref.isBlank() || this.name.isBlank() || this.email.isBlank());
    }

}
