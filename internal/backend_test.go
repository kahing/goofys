// Copyright 2020 Ka-Hing Cheung
// Copyright 2020 Databricks
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

type TestBackend struct {
	StorageBackend
	err error
}

func (s *TestBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.HeadBlob(param)
}

func (s *TestBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.ListBlobs(param)
}

func (s *TestBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.DeleteBlob(param)
}

func (s *TestBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.DeleteBlobs(param)
}

func (s *TestBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.RenameBlob(param)
}

func (s *TestBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.CopyBlob(param)
}

func (s *TestBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.GetBlob(param)
}

func (s *TestBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.PutBlob(param)
}

func (s *TestBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobBegin(param)
}

func (s *TestBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobAdd(param)
}

func (s *TestBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobAbort(param)
}

func (s *TestBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobCommit(param)
}

func (s *TestBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartExpire(param)
}
