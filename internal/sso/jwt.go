package sso

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// parseJWT decodifica JWT sem validar assinatura (para claims basicas)
func parseJWT(token string) (map[string]interface{}, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("token invalido")
	}

	payload, err := base64.RawURLEncoding.DecodeString(padJWT(parts[1]))
	if err != nil {
		return nil, err
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, err
	}
	return claims, nil
}

func padJWT(s string) string {
	for len(s)%4 != 0 {
		s += "="
	}
	return s
}