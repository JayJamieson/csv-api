package api

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/JayJamieson/csv-api/pkg/db"
	"github.com/JayJamieson/csv-api/pkg/utils"
	"github.com/labstack/echo/v4"
	"github.com/oapi-codegen/runtime/types"
)

var _ ServerInterface = (*Server)(nil)

// FetchCSV implements ServerInterface.
func (h *Server) FetchCSV(ctx echo.Context, id types.UUID, params FetchCSVParams) error {
	reqCtx := ctx.Request().Context()
	idStr := id.String()

	csvTable, err := h.db.GetCSVTable(reqCtx, idStr)

	if err != nil {
		return errorResponse(ctx, http.StatusNotFound, "Resource not found", err.Error())
	}

	columns, rows, total, queryTime, err := h.db.GetCSV(
		reqCtx, &db.QueryCSV{
			ID:         idStr,
			TableName:  csvTable.TableName,
			Limit:      params.Limit,
			Offset:     params.Offset,
			SortColumn: params.SortColumn,
			SortOrder:  string(params.SortOrder),
			Format:     string(params.Format),
		},
	)

	if err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "Query error: ", err.Error())
	}

	resp := CSVResponse{
		Total:   total,
		Ok:      true,
		QueryMs: queryTime,
		Columns: columns,
		Rows:    rows,
	}

	return ctx.JSON(http.StatusOK, resp)
}

// ImportCSV implements ServerInterface.
func (h *Server) ImportCSV(ctx echo.Context, params ImportCSVParams) error {
	reqCtx := ctx.Request().Context()

	var reader io.Reader
	var filename string

	if params.Url != "" {

		reader, err := utils.DownloadFile(params.Url)

		if err != nil {
			return errorResponse(ctx, http.StatusInternalServerError, "URL fetch error", err.Error())
		}

		defer reader.Close()

		parsedURL, err := url.Parse(params.Url)
		if err != nil {
			return errorResponse(ctx, http.StatusBadRequest, "Invalid URL", err.Error())
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
		return errorResponse(ctx, http.StatusBadRequest, "Missing import parameters",
			"Either 'url' or 'name' parameter must be provided")
	}

	csvTable, err := h.db.ImportCSVFromReader(reqCtx, filename, reader)

	if err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "CSV import error", err.Error())
	}

	endpoint := fmt.Sprintf("%s://%s/api/%s", ctx.Scheme(), ctx.Request().Host, csvTable.ID)

	return ctx.JSON(http.StatusOK, ImportResponse{
		Ok:       true,
		Endpoint: endpoint,
	})
}

func errorResponse(c echo.Context, status int, error string, message string) error {
	resp := ErrorResponse{
		Timestamp: time.Now().UTC(),
		Error:     error,
		Message:   message,
	}
	return c.JSON(status, resp)
}
