package api

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"
	"github.com/oapi-codegen/runtime/types"
)

var _ ServerInterface = (*Server)(nil)

// FetchCSV implements ServerInterface.
func (h *Server) FetchCSV(ctx echo.Context, id types.UUID, params FetchCSVParams) error {
	reqCtx := ctx.Request().Context()
	idStr := id.String()

	if idStr == "" {
		return createErrorResponse(ctx, http.StatusBadRequest, "Missing parameter", "UUID is required")
	}

	csvTable, err := h.db.GetCSVTable(reqCtx, idStr)
	if err != nil {
		return createErrorResponse(ctx, http.StatusNotFound, "Resource not found", err.Error())
	}

	sortCol := params.Sort
	limit := 500

	if sortCol == "" {
		sortCol = params.SortDesc
	}

	if params.Size > 0 {
		limit = params.Size
	}

	sortDesc := params.SortDesc != ""

	columns, objectRows, arrayRows, total, queryTime, err := h.db.QueryDuckDBTable(
		reqCtx, idStr, csvTable.TableName, limit, params.Offset, sortCol, sortDesc, params.Rowid,
	)

	if err != nil {
		return createErrorResponse(ctx, http.StatusInternalServerError, "Query error", err.Error())
	}

	if params.Shape != "objects" {
		resp := ResponseArray{
			Total:   total,
			Ok:      true,
			QueryMs: queryTime,
			Columns: columns,
			Rows:    arrayRows,
		}
		return ctx.JSON(http.StatusOK, resp)
	}

	resp := ResponseObjects{
		Total:   total,
		Ok:      true,
		QueryMs: queryTime,
		Columns: columns,
		Rows:    objectRows,
	}

	return ctx.JSON(http.StatusOK, resp)
}

// ImportCSV implements ServerInterface.
func (h *Server) ImportCSV(ctx echo.Context, params ImportCSVParams) error {
	reqCtx := ctx.Request().Context()

	var reader io.Reader
	var filename string

	if params.Url != "" {

		// TODO: Don't use default http client, add time outs for requests
		resp, err := http.Get(params.Url)
		defer resp.Body.Close()

		if err != nil {
			return createErrorResponse(ctx, http.StatusBadRequest, "URL fetch error", err.Error())
		}

		if resp.StatusCode != http.StatusOK {
			return createErrorResponse(ctx, http.StatusBadRequest, "URL fetch error",
				fmt.Sprintf("Status code: %d", resp.StatusCode))
		}

		reader = resp.Body

		parsedURL, err := url.Parse(params.Url)
		if err != nil {
			return createErrorResponse(ctx, http.StatusBadRequest, "Invalid URL", err.Error())
		}

		path := parsedURL.Path
		for i := len(path) - 1; i >= 0; i-- {
			if path[i] == '/' {
				filename = path[i+1:]
				break
			}
		}

		if filename == "" {
			filename = "downloaded.csv"
		}
	} else if params.Name != "" {
		reader = ctx.Request().Body
		filename = params.Name
	} else {
		return createErrorResponse(ctx, http.StatusBadRequest, "Missing import parameters",
			"Either 'url' or 'name' parameter must be provided")
	}

	csvTable, err := h.db.ImportCSVFromReader(reqCtx, filename, reader)

	if err != nil {
		return createErrorResponse(ctx, http.StatusInternalServerError, "CSV import error", err.Error())
	}

	endpoint := fmt.Sprintf("%s://%s/api/memory/%s", ctx.Scheme(), ctx.Request().Host, csvTable.ID)

	return ctx.JSON(http.StatusOK, ImportResponse{
		Ok:       true,
		Endpoint: endpoint,
	})
}
