package api

import (
	"net/http"

	"github.com/JayJamieson/csv-api/pkg/sniff"
	"github.com/labstack/echo/v4"
)

// ListFieldTypes implements ServerInterface: the manual mapping UI's dropdown
// fetches this instead of hardcoding a field list, so a field added via
// CreateFieldType is immediately selectable everywhere.
func (h *Server) ListFieldTypes(ctx echo.Context) error {
	fields := h.registry.Fields()
	resp := FieldTypesResponse{Ok: true}
	for _, f := range fields {
		resp.Fields = append(resp.Fields, toAPIFieldType(f))
	}
	return ctx.JSON(http.StatusOK, resp)
}

// CreateFieldType implements ServerInterface: defines a new canonical field,
// persists it, and makes it live in this process immediately (no restart
// needed to pick it up on the next /load).
func (h *Server) CreateFieldType(ctx echo.Context) error {
	reqCtx := ctx.Request().Context()

	var req CreateFieldTypeRequest
	if err := ctx.Bind(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Invalid request body", err.Error())
	}

	kind := sniff.ValueKind(req.ValueKind)
	if kind == "" {
		kind = sniff.KindText
	}
	def := sniff.FieldDef{
		Key:       sniff.CanonicalField(req.Key),
		Label:     req.Label,
		ValueKind: kind,
		Required:  req.Required,
	}

	if err := h.registryStore.AddField(reqCtx, def); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Could not create field type", err.Error())
	}
	if err := h.registry.AddField(def); err != nil {
		// Persisted but the in-memory registry rejected it (e.g. lost a race
		// with another request adding the same key first-come-first-served).
		// The DB row is the source of truth on the next restart either way.
		return errorResponse(ctx, http.StatusConflict, "Field type saved but not applied", err.Error())
	}

	got, _ := h.registry.Get(def.Key)
	return ctx.JSON(http.StatusOK, FieldTypeResponse{Ok: true, Field: toAPIFieldType(got)})
}

// AddFieldSynonym implements ServerInterface: teaches a header-name fragment
// for an existing field, e.g. accepting a corpus scan's missing-synonym
// suggestion, or a one-off supplier abbreviation.
func (h *Server) AddFieldSynonym(ctx echo.Context, key string) error {
	reqCtx := ctx.Request().Context()

	var req AddSynonymRequest
	if err := ctx.Bind(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Invalid request body", err.Error())
	}

	field := sniff.CanonicalField(key)
	if err := h.registryStore.AddSynonym(reqCtx, field, req.Synonym, "user"); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Could not add synonym", err.Error())
	}
	if err := h.registry.AddSynonym(field, req.Synonym); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Synonym saved but not applied", err.Error())
	}

	got, _ := h.registry.Get(field)
	return ctx.JSON(http.StatusOK, FieldTypeResponse{Ok: true, Field: toAPIFieldType(got)})
}

func toAPIFieldType(f sniff.FieldDef) FieldTypeDef {
	return FieldTypeDef{
		Key:        string(f.Key),
		Label:      f.Label,
		ValueKind:  FieldTypeDefValueKind(f.ValueKind),
		Required:   f.Required,
		ClaimOrder: f.ClaimOrder,
		Builtin:    f.Builtin,
	}
}
